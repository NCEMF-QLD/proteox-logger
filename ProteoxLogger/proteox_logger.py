import asyncio
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from autobahn.asyncio.wamp import ApplicationSession, ApplicationRunner
from autobahn.wamp import auth

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# ============================================================
# Proteox WAMP settings
# ============================================================
WAMP_USER = ""
WAMP_USER_SECRET = ""
WAMP_REALM = "ucss"
WAMP_ROUTER_URL = ""


# ============================================================
# InfluxDB settings
# ============================================================
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = ""
INFLUX_ORG = "ncemf"
INFLUX_BUCKET = "proteox_fridgeA"


# ============================================================
# Endpoint definitions
# ============================================================
TEMP_ENDPOINTS = {
    "PTR1-PT1-S": "oi.decs.temperature_control.PTR1_PT1_S.temperature",
    "PTR1-PT2-S": "oi.decs.temperature_control.PTR1_PT2_S.temperature",
    "DRI-PT1-S":  "oi.decs.temperature_control.DRI_PT1_S.temperature",
    "DRI-PT2-S":  "oi.decs.temperature_control.DRI_PT2_S.temperature",
    "DRI-CLD-S":  "oi.decs.temperature_control.DRI_CLD_S.temperature",
    "DRI-STL-S":  "oi.decs.temperature_control.DRI_STL_S.temperature",
    "DRI-MXC-S":  "oi.decs.temperature_control.DRI_MIX_CL.DRI_MIX_S.temperature",
    "SRB-GGS-S":  "oi.decs.temperature_control.SRB_GGS_CL.SRB_GGS_S.temperature",
}

PG_ENDPOINTS = {
    "3CL-PG01": "oi.decs.proteox.3CL_PG_01.pressure",
    "3CL-PG02": "oi.decs.proteox.3CL_PG_02.pressure",
    "3CL-PG03": "oi.decs.proteox.3CL_PG_03.pressure",
    "3CL-PG04": "oi.decs.proteox.3CL_PG_04.pressure",
    "3CL-PG05": "oi.decs.proteox.3CL_PG_05.pressure",
    "3CL-PG06": "oi.decs.proteox.3CL_PG_06.pressure",
    "3SP-PG01": "oi.decs.proteox.3SP_PG_01.pressure",
    "OVC-PG01": "oi.decs.proteox.OVC_PG_01.pressure",
}

STATE_ENDPOINT = "oi.decs.proteox.state"


# ============================================================
# State mappings
# ============================================================
PROTEOX_MAJOR_STATE_MAP = {
    0: "IDLE",
    2000: "PUMPING",
    4000: "CONDENSING",
    5000: "CIRCULATING",
    8000: "WARM_UP",
    100000: "CLEAN_TRAP_COLD",
    110000: "SAMPLE_EXCHANGE",
}

PROTEOX_DEMANDED_STATE_MAP = {
    0: "MANUAL_MODE",
    1000: "COOLDOWN",
    2000: "PUMP_DOWN",
    3000: "PRECOOL",
    4000: "CONDENSE",
    5000: "CIRCULATE",
    8000: "WARM_UP",
    9000: "COLLECT_MIX",
    100000: "CLEAN_TRAP_COLD",
    110000: "EXCHANGE_SAMPLE",
}

STATE_FIELD_MAP = {
    0: "idle",
    2000: "pumping",
    4000: "condensing",
    5000: "circulating",
    8000: "warm_up",
    100000: "clean_trap_cold",
    110000: "sample_exchange",
}


# ============================================================
# Archive settings
# ============================================================
ARCHIVE_ROOT = Path("proteox_archive")
ARCHIVE_ROOT.mkdir(exist_ok=True)

PARQUET_FLUSH_EVERY_N_ROWS = 500
PARQUET_FLUSH_EVERY_N_SECONDS = 15

INFLUX_FLUSH_EVERY_N_ROWS = 500
INFLUX_FLUSH_EVERY_N_SECONDS = 5


# ============================================================
# Queues
# ============================================================
intake_queue = None
parquet_queue = None
influx_queue = None


# ============================================================
# Helpers
# ============================================================
def normalize_event(sensor_name, topic, measurement, args):
    """
    Temperature / pressure shape:
        (status1, status2, ts_sec, ts_ns, value, valid)
    """
    ts_sec = int(args[2])
    ts_ns = int(args[3])
    value = float(args[4])
    valid = bool(args[5]) if len(args) > 5 else True

    unix_ns = ts_sec * 1_000_000_000 + ts_ns
    time_local = datetime.fromtimestamp(ts_sec + ts_ns * 1e-9)

    return {
        "time": time_local,
        "unix_ns": unix_ns,
        "sensor": sensor_name,
        "topic": topic,
        "measurement": measurement,
        "value": value,
        "valid": valid,
    }


def normalize_state_event(topic, args):
    """
    Observed state shape:
        (record_type, ts_high, ts_low, ts_ns, major_state, minor_state, demanded_state)

    If timestamp is zero, use local receipt time.
    """
    record_type = int(args[0])
    ts_high = int(args[1])
    ts_low = int(args[2])
    ts_ns = int(args[3])

    major_state = int(args[4])
    minor_state = int(args[5])
    demanded_state = int(args[6])

    if ts_high == 0 and ts_low == 0 and ts_ns == 0:
        time_local = datetime.now()
        unix_ns = int(time_local.timestamp() * 1_000_000_000)
        timestamp_source = "local_receipt_time"
    else:
        unix_time = ts_high + ts_ns * 1e-9
        time_local = datetime.fromtimestamp(unix_time)
        unix_ns = ts_high * 1_000_000_000 + ts_ns
        timestamp_source = "proteox_timestamp"

    return {
        "time": time_local,
        "unix_ns": unix_ns,
        "topic": topic,
        "measurement": "state",
        "record_type": record_type,
        "timestamp_source": timestamp_source,
        "major_state": major_state,
        "major_state_name": PROTEOX_MAJOR_STATE_MAP.get(
            major_state, f"UNKNOWN_{major_state}"
        ),
        "minor_state": minor_state,
        "demanded_state": demanded_state,
        "demanded_state_name": PROTEOX_DEMANDED_STATE_MAP.get(
            demanded_state, f"UNKNOWN_{demanded_state}"
        ),
    }


def daily_output_path(record_time: datetime) -> Path:
    month_dir = ARCHIVE_ROOT / record_time.strftime("%Y-%m")
    month_dir.mkdir(parents=True, exist_ok=True)
    return month_dir / f"proteox_{record_time.strftime('%Y-%m-%d')}.parquet"


def daily_state_output_path(record_time: datetime) -> Path:
    month_dir = ARCHIVE_ROOT / record_time.strftime("%Y-%m")
    month_dir.mkdir(parents=True, exist_ok=True)
    return month_dir / f"proteox_state_{record_time.strftime('%Y-%m-%d')}.parquet"


# ============================================================
# Daily parquet appender
# ============================================================
class DailyParquetAppender:
    def __init__(self, path_builder):
        self.current_date = None
        self.current_path = None
        self.writer = None
        self.schema = None
        self.path_builder = path_builder

    def append_records(self, records):
        if not records:
            return

        df = pd.DataFrame(records)
        record_date = df["time"].iloc[0].date()

        if self.current_date != record_date:
            self.close()
            self.current_date = record_date
            self.current_path = self.path_builder(df["time"].iloc[0])

        table = pa.Table.from_pandas(df, preserve_index=False)

        if self.writer is None:
            self.schema = table.schema
            self.writer = pq.ParquetWriter(
                where=str(self.current_path),
                schema=self.schema,
                compression="zstd",
            )
            print(f"[archive] writing to {self.current_path}")

        self.writer.write_table(table)

    def close(self):
        if self.writer is not None:
            self.writer.close()
            print(f"[archive] closed {self.current_path}")
            self.writer = None
            self.schema = None
            self.current_path = None
            self.current_date = None


# ============================================================
# WAMP collector
# ============================================================
class ProteoxCollector(ApplicationSession):
    def onConnect(self):
        print("[wamp] connecting...")
        self.join(WAMP_REALM, ["wampcra"], WAMP_USER)

    def onChallenge(self, challenge):
        return auth.compute_wcs(
            WAMP_USER_SECRET.encode(),
            challenge.extra["challenge"]
        )

    def _reset_cooldown_tracker(self, now):
        self.cooldown_start_time = now
        self.prev_major_state = None
        self.prev_demanded_state = None
        self.current_major_state = None
        self.current_major_started_at = None
        self.major_totals_s = defaultdict(int)

    def _update_state_elapsed(self, event):
        now = event["time"]
        major = event["major_state"]
        demanded = event["demanded_state"]

        new_cooldown = (
            self.cooldown_start_time is None
            or (self.prev_demanded_state != 1000 and demanded == 1000)
            or (
                self.prev_major_state in {0, 8000, 110000}
                and major in {2000, 4000, 5000}
            )
        )

        if new_cooldown:
            self._reset_cooldown_tracker(now)

        if self.current_major_state is None:
            self.current_major_state = major
            self.current_major_started_at = now

        elif major != self.current_major_state:
            elapsed_prev = int((now - self.current_major_started_at).total_seconds())
            if elapsed_prev > 0:
                self.major_totals_s[self.current_major_state] += elapsed_prev

            self.current_major_state = major
            self.current_major_started_at = now

        current_major_elapsed_s = int((now - self.current_major_started_at).total_seconds())
        cooldown_elapsed_s = int((now - self.cooldown_start_time).total_seconds())

        totals_snapshot = dict(self.major_totals_s)
        totals_snapshot[major] = totals_snapshot.get(major, 0) + current_major_elapsed_s

        event["major_state_elapsed_s"] = current_major_elapsed_s
        event["cooldown_elapsed_s"] = cooldown_elapsed_s

        for code, slug in STATE_FIELD_MAP.items():
            event[f"{slug}_elapsed_s"] = int(totals_snapshot.get(code, 0))

        self.prev_major_state = major
        self.prev_demanded_state = demanded

        return event

    async def state_poller(self):
        while True:
            try:
                resp = await self.call(STATE_ENDPOINT)
                event = normalize_state_event(STATE_ENDPOINT, resp.results)
                event = self._update_state_elapsed(event)
                await intake_queue.put(event)
            except Exception as e:
                print(f"[state poll] error: {e}")

            await asyncio.sleep(5)

    async def onJoin(self, details):
        print("[wamp] joined realm, subscribing to topics...")

        self._reset_cooldown_tracker(datetime.now())

        # Temperature subscriptions
        for sensor_name, topic in TEMP_ENDPOINTS.items():

            async def temp_handler(*args, sensor_name=sensor_name, topic=topic, **kwargs):
                try:
                    event = normalize_event(sensor_name, topic, "temperature", args)
                    await intake_queue.put(event)
                except Exception as e:
                    print(f"[handler] temperature error for {sensor_name}: {e}")

            await self.subscribe(temp_handler, topic)

        # Pressure subscriptions
        for sensor_name, topic in PG_ENDPOINTS.items():

            async def pg_handler(*args, sensor_name=sensor_name, topic=topic, **kwargs):
                try:
                    event = normalize_event(sensor_name, topic, "pressure", args)
                    await intake_queue.put(event)
                except Exception as e:
                    print(f"[handler] pressure error for {sensor_name}: {e}")

            await self.subscribe(pg_handler, topic)

        print("[wamp] starting state poller")
        self.state_task = asyncio.create_task(self.state_poller())

        print(
            f"[wamp] streaming {len(TEMP_ENDPOINTS)} temperature topics, "
            f"{len(PG_ENDPOINTS)} pressure topics, and polled proteox state"
        )

    def onDisconnect(self):
        print("[wamp] disconnected")
        if hasattr(self, "state_task"):
            self.state_task.cancel()


# ============================================================
# Dispatcher
# ============================================================
async def dispatcher():
    try:
        while True:
            event = await intake_queue.get()

            await parquet_queue.put(event.copy())
            await influx_queue.put(event.copy())

            intake_queue.task_done()

    except asyncio.CancelledError:
        print("[dispatcher] stopped")
        raise


# ============================================================
# Parquet writer
# ============================================================
async def parquet_writer():
    main_buffer = []
    state_buffer = []

    main_appender = DailyParquetAppender(daily_output_path)
    state_appender = DailyParquetAppender(daily_state_output_path)

    last_flush = asyncio.get_running_loop().time()

    def flush_buffers():
        nonlocal main_buffer, state_buffer, last_flush

        if main_buffer:
            main_appender.append_records(main_buffer)
            print(f"[archive] flushed {len(main_buffer)} temp/pressure rows")
            main_buffer.clear()

        if state_buffer:
            state_appender.append_records(state_buffer)
            print(f"[archive] flushed {len(state_buffer)} state rows")
            state_buffer.clear()

        last_flush = asyncio.get_running_loop().time()

    try:
        while True:
            timeout = PARQUET_FLUSH_EVERY_N_SECONDS - (
                asyncio.get_running_loop().time() - last_flush
            )
            timeout = max(timeout, 0.1)

            try:
                event = await asyncio.wait_for(parquet_queue.get(), timeout=timeout)

                if event["measurement"] == "state":
                    state_buffer.append(event)
                else:
                    main_buffer.append(event)

                parquet_queue.task_done()

            except asyncio.TimeoutError:
                pass

            now = asyncio.get_running_loop().time()
            total_rows = len(main_buffer) + len(state_buffer)

            should_flush = (
                total_rows >= PARQUET_FLUSH_EVERY_N_ROWS or
                ((main_buffer or state_buffer) and (now - last_flush) >= PARQUET_FLUSH_EVERY_N_SECONDS)
            )

            if should_flush:
                flush_buffers()

    except asyncio.CancelledError:
        print("[archive] stopping, flushing remaining buffered data...")
        flush_buffers()
        main_appender.close()
        state_appender.close()
        raise


# ============================================================
# Influx writer
# ============================================================
async def influx_writer():
    client = InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG,
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)

    buffer = []
    last_flush = asyncio.get_running_loop().time()

    def flush_buffer():
        nonlocal buffer, last_flush

        if not buffer:
            return

        points = []

        for event in buffer:
            if event["measurement"] == "state":
                point = (
                    Point("proteox_state")
                    .tag("system", "proteox")
                    .field("major_state", int(event["major_state"]))
                    .field("minor_state", int(event["minor_state"]))
                    .field("demanded_state", int(event["demanded_state"]))
                    .field("major_state_elapsed_s", int(event["major_state_elapsed_s"]))
                    .field("cooldown_elapsed_s", int(event["cooldown_elapsed_s"]))
                    .field("idle_elapsed_s", int(event["idle_elapsed_s"]))
                    .field("pumping_elapsed_s", int(event["pumping_elapsed_s"]))
                    .field("condensing_elapsed_s", int(event["condensing_elapsed_s"]))
                    .field("circulating_elapsed_s", int(event["circulating_elapsed_s"]))
                    .field("warm_up_elapsed_s", int(event["warm_up_elapsed_s"]))
                    .field("clean_trap_cold_elapsed_s", int(event["clean_trap_cold_elapsed_s"]))
                    .field("sample_exchange_elapsed_s", int(event["sample_exchange_elapsed_s"]))
                    .time(int(event["unix_ns"]), WritePrecision.NS)
                )
            else:
                point = (
                    Point(f"proteox_{event['measurement']}")
                    .tag("sensor", event["sensor"])
                    .field("value", float(event["value"]))
                    .field("valid", bool(event["valid"]))
                    .time(int(event["unix_ns"]), WritePrecision.NS)
                )

            points.append(point)

        write_api.write(
            bucket=INFLUX_BUCKET,
            org=INFLUX_ORG,
            record=points,
        )

        print(f"[influx] wrote {len(points)} points")

        buffer.clear()
        last_flush = asyncio.get_running_loop().time()

    try:
        while True:
            timeout = INFLUX_FLUSH_EVERY_N_SECONDS - (
                asyncio.get_running_loop().time() - last_flush
            )
            timeout = max(timeout, 0.1)

            try:
                event = await asyncio.wait_for(influx_queue.get(), timeout=timeout)
                buffer.append(event)
                influx_queue.task_done()

            except asyncio.TimeoutError:
                pass

            now = asyncio.get_running_loop().time()
            should_flush = (
                len(buffer) >= INFLUX_FLUSH_EVERY_N_ROWS or
                (buffer and (now - last_flush) >= INFLUX_FLUSH_EVERY_N_SECONDS)
            )

            if should_flush:
                flush_buffer()

    except asyncio.CancelledError:
        print("[influx] stopping, flushing remaining buffered data...")
        flush_buffer()
        client.close()
        raise


# ============================================================
# Main
# ============================================================
async def async_main():
    global intake_queue, parquet_queue, influx_queue

    intake_queue = asyncio.Queue()
    parquet_queue = asyncio.Queue()
    influx_queue = asyncio.Queue()

    dispatcher_task = asyncio.create_task(dispatcher())
    parquet_task = asyncio.create_task(parquet_writer())
    influx_task = asyncio.create_task(influx_writer())

    try:
        runner = ApplicationRunner(WAMP_ROUTER_URL, WAMP_REALM)
        await runner.run(ProteoxCollector, start_loop=False)

        while True:
            await asyncio.sleep(1)

    finally:
        for task in [dispatcher_task, parquet_task, influx_task]:
            task.cancel()

        for task in [dispatcher_task, parquet_task, influx_task]:
            try:
                await task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(async_main())