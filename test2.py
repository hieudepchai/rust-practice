import copy
import hashlib
import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Union

# import iso8601
from dateutil.tz import tzoffset
# from loguru import logger

from enum import Enum
from dataclasses import dataclass

class TransformationException(Exception):
    pass


TIMEZONE_COUNTRY = {}

class FieldTransformer:
    import copy
    import hashlib
    import json
    import math
    import os
    import re
    import time
    from datetime import datetime, timedelta, timezone
    from typing import Union

    class FirebaseUtils:
        @staticmethod
        def get_brand_from_project(project_name: str, default_value=None) -> any:
            FIREBASE_PROJECT_BRAND_MAPPER = {'com.armaniexchange.connected': {'brand': 'ax', 'env': 'prd'}, 'com.misfit.armaniexchange.staging': {'brand': 'ax', 'env': 'stg'}, 'com.misfit.armaniexchange.connected': {'brand': 'ax', 'env': 'stg'}, 'com.chaps.connected': {'brand': 'chaps', 'env': 'prd'}, 'com.misfit.chaps.staging': {'brand': 'chaps', 'env': 'stg'}, 'com.misfit.chaps.connected': {'brand': 'chaps', 'env': 'stg'}, 'com.diesel.on': {'brand': 'diesel', 'env': 'prd'}, 'com.misfit.diesel': {'brand': 'diesel', 'env': 'stg'}, 'com.misfit.diesel.staging': {'brand': 'diesel', 'env': 'stg'}, 'com.emporioarmani.connected': {'brand': 'ea', 'env': 'prd'}, 'com.misfit.emporioarmani.staging': {'brand': 'ea', 'env': 'stg'}, 'com.misfit.emporioarmani.connected': {'brand': 'ea', 'env': 'stg'}, 'com.fossil.q': {'brand': 'fossil', 'env': 'prd'}, 'com.fossil.qlegacy': {'brand': 'fossil', 'env': 'prd'}, 'com.misfit.fossil.legacy': {'brand': 'fossil', 'env': 'prd'}, 'com.fossil.wearables.fossil': {'brand': 'fossil', 'env': 'prd'}, 'com.fossil.wearables.fossil.staging': {'brand': 'fossil', 'env': 'stg'}, 'com.fossil.qlegacy.staging': {
                'brand': 'fossil', 'env': 'stg'}, 'com.misfit.fossilq.staging': {'brand': 'fossil', 'env': 'stg'}, 'com.fossil.diana.debug': {'brand': 'fossil', 'env': 'stg'}, 'com.katespade.connected': {'brand': 'ks', 'env': 'prd'}, 'com.misfit.katespade.staging': {'brand': 'ks', 'env': 'stg'}, 'com.marcjacobs.mj': {'brand': 'mj', 'env': 'prd'}, 'com.misfit.marcjacobs.mj.staging': {'brand': 'mj', 'env': 'stg'}, 'com.misfit.marcjacobs.mj': {'brand': 'mj', 'env': 'stg'}, 'com.michaelkors.access': {'brand': 'mk', 'env': 'prd'}, 'com.misfit.michaelkors.staging': {'brand': 'mk', 'env': 'stg'}, 'com.misfit.portfolio.staging': {'brand': 'portfolio', 'env': 'stg'}, 'com.misfit.portfolio.diana.debug': {'brand': 'portfolio', 'env': 'stg'}, 'com.misfit.portfolio.diana.staging': {'brand': 'portfolio', 'env': 'stg'}, 'com.relic.connected': {'brand': 'relic', 'env': 'prd'}, 'com.misfit.relic.staging': {'brand': 'relic', 'env': 'prd'}, 'com.misfit.relic.connected': {'brand': 'relic', 'env': 'prd'}, 'com.skagen.connected': {'brand': 'skagen', 'env': 'prd'}, 'com.misfit.skagen.staging': {'brand': 'skagen', 'env': 'stg'}, 'com.misfit.skagen.connected': {'brand': 'skagen', 'env': 'stg'}}
            brand_info = FIREBASE_PROJECT_BRAND_MAPPER.get(project_name, None)
            if brand_info is None:
                return default_value
            return brand_info.get('brand')

    SQL_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
    DATE_INDEX_FORMAT = "%Y%m%d"
    TIME_INDEX_FORMAT = "%H%M%S"
    MAX_SECOND_TIMESTAMP = 9_999_999_999
    EXEC_SAFE_LIST = ["math", "datetime", "json", "re"]
    BRANDS = [
        'universal_citizen', 'citizen', 'morellato', 'nixon', 'skagen',
        'relic', 'diesel', 'tb', 'ks', 'mk', 'dkny', 'michele', 'ea', 'chaps',
        'ax', 'fossil', 'mj', 'universal', 'portfolio'
    ]
    EXEC_SAFE_DICT = dict([(k, globals().get(k, None))
                        for k in EXEC_SAFE_LIST])
    CUSTOM_CODE = {}

    VERSION_PATTERN_1 = re.compile(r"^\d{1,3}$")
    VERSION_PATTERN_2 = re.compile(r"^\d{1,3}\.\d{1,3}$")
    VERSION_PATTERN_3 = re.compile(r"^(\d{1,3}\.\d{1,3}\.\d{1,3})((-|\.).*)?$")
    DUMMY_EMAIL = re.compile(
        r"(.*@misfitqa.com|.*@cloudtestlabaccounts.com"
        r"|.*@fossilqa.com|.*miltest.*|.*loadtest.*|.*stress-test.*|.*autotest.*|.*integrationtestcloud.*)"
    )

    DUMMY_DEVICE = re.compile(r"(.*TEST.*|.*INGTE.*)")

    ATE_PATTERN_STANDARD_1 = re.compile(
        r"([\w]+)[.]([a-zA-Z]+)[.]([\w]+)-([\w]*)[.]([0-9]+)[.]([a-zA-Z]+)",
        re.I)
    ATE_PATTERN_STANDARD_2 = re.compile(
        r"([\w]+)[.]([\w]+)[.]([a-zA-Z]*)([0-9]*)", re.I)
    ATE_PATTERN_STANDARD_3 = re.compile(
        r"([\w]+)[.]([a-zA-Z]+)[.]([\w]+)[.]([0-9]*)-([a-zA-Z]+)[.]([a-zA-Z]+)",
        re.I)
    ATE_PATTERN_2_5_1 = re.compile(
        r"([\w]+)[.][2][.][5][.]([a-zA-Z]*)([0-9]+)", re.I)
    ATE_PATTERN_2_5_2 = re.compile(
        r"([\w]+)[.][W][.][2][.][5]-([\w]+)[.]([0-9]+)[.]([\w]+)", re.I)

    def __init__(self, strict=True):
        self._is_strict = strict

    def transform(self, value: any, method_name: str, **params: any) -> any:
        method = getattr(self, method_name)

        if method:
            return method(value, **params)

        if self._is_strict:
            raise TransformationException(
                "Transformation method doesn't exist")

        return copy.copy(value)

    def json_object_to_string(self, value: dict) -> str:
        if isinstance(value, str):
            return value

        string_obj = json.dumps(value)

        if isinstance(string_obj, bytes):
            # This function never jump in to this condition because json.dumps return string type
            return string_obj.decode("utf-8")

        return string_obj

    def json_object_hash(self, value: dict) -> str:
        hashed = hashlib.md5(
            str(json.dumps(value, sort_keys=True)).encode('utf-8'))
        return hashed.hexdigest()

    def normalize_datetime(self, value: any, datetime_format: str) -> datetime:

        if isinstance(value, str):
            datetime_value = datetime.strptime(value, datetime_format)
        elif isinstance(value, datetime):
            datetime_value = value
        else:
            raise TransformationException(
                f"Input format error, "
                f"expected format: {datetime_format} of type string")

        return self.datetime_to_db_datetime(datetime_value)

    def convert_bson_object_id(self, value: Union[str, dict]) -> str:

        if isinstance(value, str):
            return value

        if isinstance(value, object):
            if "$oid" in value:
                return str(value["$oid"])

        return None

    def convert_bson_long_datetime(self, value: Union[str, int, dict]) -> str:

        timestamp = None

        if isinstance(value, str):
            timestamp = int(value)

        if isinstance(value, int):
            timestamp = value

        if isinstance(value, dict):
            if "$date" in value:
                if isinstance(value["$date"], int):
                    timestamp = value["$date"]
                elif isinstance(value["$date"], str):
                    timestamp = self._rfc3339_to_timestamp(value["$date"])
                elif isinstance(value["$date"], datetime):
                    epoch = datetime(1970, 1, 1)
                    timestamp = (value["$date"] - epoch).total_seconds()
                elif "$numberLong" in value["$date"]:
                    timestamp = int(value["$date"]["$numberLong"])

            if "$numberLong" in value:
                if isinstance(value["$numberLong"],
                            str) and value["$numberLong"].isdigit():
                    timestamp = int(value["$numberLong"])

        if timestamp:
            while timestamp > self.MAX_SECOND_TIMESTAMP:
                timestamp = timestamp / 1000  # Convert to second

            return self._epoch_to_db_datetime(timestamp)

        return None

    def convert_bson_rfc3339_datetime(self,
                                    value: Union[str, dict],
                                    keep_zero_microsecond=True) -> str:

        if isinstance(value, dict) and "$date" in value:
            if isinstance(value["$date"], datetime):
                value["$date"] = value["$date"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%f")

        if value and isinstance(value, datetime):
            value = value.strftime("%Y-%m-%dT%H:%M:%S.%f")

        if isinstance(value, str):
            return self._rfc3339_datetime_to_db_datetime(
                value, keep_zero_microsecond)

        if "$date" in value:
            return self._rfc3339_datetime_to_db_datetime(
                value["$date"], keep_zero_microsecond)

        return None

    def to_date_index(self, value: Union[str, dict]) -> int:

        parsed_date = None

        if value and isinstance(value, dict) and "$date" in value:
            if isinstance(value["$date"], datetime):
                parsed_date = value["$date"]

                return self.datetime_to_date_index(parsed_date)

        if value and isinstance(value, datetime):
            parsed_date = value

            return self.datetime_to_date_index(parsed_date)

        if value and isinstance(value, str):
            if value.endswith("Z"):
                value = value[:-1]
            parsed_date = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")
        elif value and "$date" in value:
            if isinstance(value["$date"], str):
                if value["$date"].endswith("Z"):
                    value["$date"] = value["$date"][:-1]
                try:
                    parsed_date = datetime.strptime(value["$date"],
                                                    "%Y-%m-%dT%H:%M:%S.%f")
                except Exception as _:
                    parsed_date = datetime.strptime(value["$date"],
                                                    "%Y-%m-%dT%H:%M:%S")

            if isinstance(value["$date"], int):
                parsed_date = datetime.utcfromtimestamp(value["$date"] / 1000)

        if parsed_date:
            return self.datetime_to_date_index(parsed_date)

        return 0

    def bson_date_to_datetime(self, value: Union[str, dict]) -> datetime:

        parsed_date = None

        if value and isinstance(value, dict) and "$date" in value:
            if isinstance(value["$date"], datetime):
                value["$date"] = value["$date"].strftime(
                    "%Y-%m-%dT%H:%M:%S.%f")

        if value and isinstance(value, datetime):
            value = value.strftime("%Y-%m-%dT%H:%M:%S.%f")

        if value and isinstance(value, str):
            if value.endswith("Z"):
                value = value[:-1]
            parsed_date = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")
        elif value and "$date" in value:
            if isinstance(value["$date"], str):
                if value["$date"].endswith("Z"):
                    value["$date"] = value["$date"][:-1]
                try:
                    parsed_date = datetime.strptime(value["$date"],
                                                    "%Y-%m-%dT%H:%M:%S.%f")
                except Exception as _:
                    parsed_date = datetime.strptime(value["$date"],
                                                    "%Y-%m-%dT%H:%M:%S")

            if isinstance(value["$date"], int):
                parsed_date = datetime.utcfromtimestamp(value["$date"] / 1000)

        return parsed_date

    def bson_date_to_time_index(self, value: Union[str, dict]) -> int:

        timestamp = None

        if value and isinstance(value, dict) and "$date" in value:
            if isinstance(value["$date"], datetime):
                timestamp = value["$date"]

                return self.datetime_to_time_index(timestamp)

        if value and isinstance(value, datetime):
            timestamp = value

            return self.datetime_to_time_index(timestamp)

        if isinstance(value, str):
            timestamp = self.date_string_to_datetime(value)
        elif "$date" in value:
            if isinstance(value["$date"], str):
                if value["$date"].endswith("Z"):
                    value["$date"] = value["$date"][:-1]
                timestamp = self.date_string_to_datetime(value["$date"])

            if isinstance(value["$date"], int):
                timestamp = datetime.utcfromtimestamp(value["$date"] / 1000)

        if timestamp:
            return self.datetime_to_time_index(timestamp)

        return None

    def date_string_to_datetime(self, value: str) -> datetime:
        timestamp = None
        try:
            timestamp = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f")
        except Exception as _:
            timestamp = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")

        return timestamp

    # def iso_8601_to_date_index(self, value: any) -> int:
    #     """
    #     Convert iso 8601 datetime to date index
    #     :param value: iso 8601 datetime string. e.g. 2019-03-18T17:41:54.000Z
    #     :return: date index. e.g. 20190318

    #     if not value:
    #         return None
    #     parsed_date = iso8601.parse_date(value)

    #     return self.datetime_to_date_index(parsed_date)

    # def iso_8601_to_time_index(self, value: any) -> int:
    #     """
    #     Convert iso 8601 datetime to time index
    #     :param value: iso 8601 datetime string. e.g. 2019-03-18T17:41:54.000Z
    #     :return: time index. e.g. 174154
    #     """

    #     if not value:
    #         return None
    #     parsed_date = iso8601.parse_date(value)

    #     return self.datetime_to_time_index(parsed_date)

    # def _rfc3339_datetime_to_db_datetime(self,
    #                                      value: any,
    #                                      keep_zero_microsecond=True) -> str:
    #

    #     if not value:
    #         return None

    #     if isinstance(value, int):
    #         parsed_date = datetime.fromtimestamp(
    #             self.normalize_second_timestamp(value))
    #     else:
    #         parsed_date = iso8601.parse_date(value)

    #     return self.datetime_to_db_datetime(parsed_date, keep_zero_microsecond)

    def _rfc3339_to_timestamp(self, value: str) -> float:

        if not value:
            return None

        error = None

        for fmt_str in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"]:
            if value.endswith("Z"):
                fmt_str = f"{fmt_str}Z"

            try:
                parsed_dt = datetime.strptime(value, fmt_str)

                # Return UTC timestamp
                epoch = datetime(1970, 1, 1)

                return (parsed_dt - epoch).total_seconds()
            except Exception as err:
                error = err

                continue

        if error:
            # logger.error(error)
            pass

        return None

    def _epoch_to_db_datetime(self, value: int) -> str:

        if not value:
            return None
        new_time = datetime.utcfromtimestamp(int(value))

        return self.datetime_to_db_datetime(new_time,
                                            keep_zero_microsecond=False)

    def datetime_to_db_datetime(self,
                                value: datetime,
                                keep_zero_microsecond=True) -> str:

        if not keep_zero_microsecond or value.microsecond == 0:
            return "%04d-%02d-%02d %02d:%02d:%02d" % (
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
            )

        return "%04d-%02d-%02d %02d:%02d:%02d%s" % (
            value.year,
            value.month,
            value.day,
            value.hour,
            value.minute,
            value.second,
            ("%.6f" % (value.microsecond / 1000000))[1:],
        )

    def datetime_to_date_index(self, value: datetime) -> int:

        return int("%04d%02d%02d" % (value.year, value.month, value.day))

    def datetime_to_time_index(self, value: datetime) -> int:

        return int("%02d%02d%02d" % (value.hour, value.minute, value.second))

    def datetime_to_utc_offset(self, value: datetime, offset: int) -> datetime:

        if offset < -12 or offset > 14:
            return None

        return value + timedelta(hours=offset)

    def unix_timestamp_to_db_date_time(self, value: float) -> str:

        if not value:
            return None

        timestamp = self.normalize_second_timestamp(float(value))

        return self.datetime_to_db_datetime(
            datetime.utcfromtimestamp(timestamp))

    def unix_timestamp_to_date_index(self, value: float) -> int:

        if not value:
            return None
        timestamp = self.normalize_second_timestamp(float(value))

        return self.datetime_to_date_index(
            datetime.utcfromtimestamp(timestamp))

    def unix_timestamp_to_time_index(self, value: float) -> int:

        if not value:
            return None
        timestamp = self.normalize_second_timestamp(float(value))

        return self.datetime_to_time_index(
            datetime.utcfromtimestamp(timestamp))

    def http_date_to_db_date_time(self, value: str) -> str:

        if not value:
            return None
        parsed_date = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %Z")

        return self.datetime_to_db_datetime(parsed_date)

    def http_date_to_date_index(self, value: str) -> int:

        if not value:
            return None
        parsed_date = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %Z")

        return self.datetime_to_date_index(parsed_date)

    def hwlog_time_to_local_date_index(self, value: str,
                                    tz_offset_in_minute: int) -> int:

        if not value:
            return None
        value = value.replace("GMT", "+0000")
        parsed_date = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %z")

        return self.datetime_to_date_index(
            parsed_date.astimezone(tz=tzoffset(None, tz_offset_in_minute *
                                            60)))

    def hwlog_time_to_local_time_index(self, value: str,
                                    tz_offset_in_minute: int) -> int:

        if not value:
            return None
        value = value.replace("GMT", "+0000")
        parsed_date = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %z")

        return self.datetime_to_time_index(
            parsed_date.astimezone(tz=tzoffset(None, tz_offset_in_minute *
                                            60)))

    def timestamp_to_local_date_index(self, value: float,
                                    tz_offset_in_second: int) -> int:

        if not value:
            return None
        parsed_date = datetime.fromtimestamp(
            self.normalize_second_timestamp(float(value)))

        return self.datetime_to_date_index(
            parsed_date.astimezone(timezone.utc).astimezone(
                tz=tzoffset(None, tz_offset_in_second)))

    def timestamp_to_local_time_index(self, value: float,
                                    tz_offset_in_second: int) -> int:

        if not value:
            return None
        parsed_date = datetime.fromtimestamp(
            self.normalize_second_timestamp(float(value)))

        return self.datetime_to_time_index(
            parsed_date.astimezone(tz=tzoffset(None, tz_offset_in_second)))

    def http_date_to_time_index(self, value: str) -> int:

        if not value:
            return None
        parsed_date = datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %Z")

        return self.datetime_to_time_index(parsed_date)

    def normalize_millisecond_timestamp(self, value: Union[float, int]) -> int:

        timestamp = float(value)

        if timestamp < self.MAX_SECOND_TIMESTAMP:
            return timestamp * 1000  # Convert to millisecond

        return timestamp

    def normalize_second_timestamp(self, value: Union[float, int]) -> float:

        timestamp = float(value)

        while timestamp > self.MAX_SECOND_TIMESTAMP:
            timestamp = timestamp / 1000  # Convert to second

        return timestamp

    def epoch_to_iso_8601(self, value: Union[str, dict]) -> str:

        ts = self.normalize_second_timestamp(value["$date"])
        dt = datetime.utcfromtimestamp(ts)
        iso_format = dt.isoformat() + "Z"

        return iso_format

    def extract_sku(self, value: str) -> str:

        if isinstance(value, str):
            return value[:6]

        return None

    def stringify(self, value: any) -> str:

        return str(value)

    def lowercase(self, value: str) -> str:

        return str(value).lower()

    def uppercase(self, value: str) -> str:

        return str(value).upper()

    def string_hash_256(self, value: str) -> str:

        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def string_to_int(self, value: str) -> int:

        try:
            return int(value)
        except Exception as e:
            return None

    def string_to_float(self, value: str) -> float:

        try:
            return float(value)
        except Exception as e:
            return None

    def ceil(self, value: float) -> int:

        try:
            return math.ceil(value)
        except:
            return None

    def floor(self, value: float) -> int:

        try:
            return math.floor(value)
        except:
            return None

    def transform_number_to_boolean(self, value: Union[float, int]) -> bool:
        if value != 0:
            return True

        return False

    def transform_with_custom_code(self,
                                value: any,
                                custom_code: str,
                                custom_variables: dict = {}) -> any:
        # http://lybniz2.sourceforge.net/safeeval.html

        safe_dict = copy.copy(self.EXEC_SAFE_DICT)
        safe_dict["field_transformer"] = self
        safe_dict.update(custom_variables)

        custom_code_id = id(custom_code)

        if custom_code_id not in self.CUSTOM_CODE:
            # custom_code is array of line of code

            if isinstance(custom_code, list):
                custom_code = "\r\n".join(custom_code)
            self.CUSTOM_CODE[custom_code_id] = compile(custom_code,
                                                    f"{custom_code_id}.py",
                                                    "exec")

        code_obj = self.CUSTOM_CODE[custom_code_id]

        exec(code_obj, {"value": value}, safe_dict)

        return safe_dict.get("output")

    def get_current_timestamp(self, value: any) -> float:
        return time.time() * 1000

    def get_current_datetime(self, value: any) -> datetime:
        return datetime.now()

    def normalize_brand_id(self, value: str) -> str:
        self.BRANDS.sort(key=len, reverse=True)
        namespace = os.getenv('NAMESPACE', 'aws_fossil').lower()
        for brand in self.BRANDS:
            if brand in value:
                if brand == 'universal' and namespace == 'aws_citizen':
                    return 'universal_citizen'
                elif brand == 'universal' and (namespace == 'aws_fossil' or
                                            namespace == 'aliyun_fossil'):
                    return 'universal'
                else:
                    return brand

        return value

    def serial_number_to_product_code(self, value: str) -> str:
        regex = r"^[WZDLM][0-9][CDEFHJKLMRSTWXYZ][0-Z]{3}"

        if isinstance(value, str) and re.match(regex, value):
            return value[:6]

        return "unknown"

    def serial_number_to_unique_identifier(self, value: str) -> str:
        regex = r"^[WZDLM][0-9][CDEFHJKLMRSTWXYZ][0-Z]{3}"

        if isinstance(value, str) and re.match(regex, value):
            return value[6:10]

        return None

    def serial_number_to_owner(self, value: str) -> str:
        if not value:
            return None

        if value[1] == "1":
            return "citizen"

        return "fossil"

    def calculate_duration(self, record: dict, **params) -> float:
        start_timestamp = self.normalize_second_timestamp(
            record.get(params.get("start"), 0))
        end_timestamp = self.normalize_second_timestamp(
            record.get(params.get("end"), 0))

        return end_timestamp - start_timestamp

    def normalize_version(self, value: str) -> str:
        value = str(value).replace(" ", "")

        if self.VERSION_PATTERN_1.match(value):
            return f"{value}.0.0"
        if self.VERSION_PATTERN_2.match(value):
            return f"{value}.0"
        if self.VERSION_PATTERN_3.match(value):
            return self.VERSION_PATTERN_3.match(value).groups()[0]

        return None

    def transform_project_name_to_brand(self, value: str) -> str:
        return self.FirebaseUtils.get_brand_from_project(value)

    def transform_db_name_to_brand(self, value: str) -> str:
        self.BRANDS.sort(key=len, reverse=True)
        namespace = os.getenv('NAMESPACE', 'aws_fossil').lower()
        for brand in self.BRANDS:
            if brand in value:
                if brand == 'universal' and namespace == 'aws_citizen':
                    return 'universal_citizen'
                elif brand == 'universal' and (namespace == 'aws_fossil' or
                                            namespace == 'aliyun_fossil'):
                    return 'universal'
                else:
                    return brand

        return value

    def escape_special_character(self, value: str) -> str:

        return str(value).encode("unicode_escape").decode("utf-8")

    def remove_spaces(self, value: str) -> str:

        return re.sub(r"\s+", " ", value)

    def extract_error_code(self, value, **params):
        part = params.get("part")

        if value == "" and part == "step":
            return None

        if value == "" and part == "code":
            return 0  # success

        if value is not None and len(value) == 7 and value.isdigit():
            if part == "step":
                return int(value[:3])

            if part == "code":
                return int(value[-4:])

        return -1

    def extract_app_log_error_code(self, value, **params):
        code_field = params.get("code_field")
        part = params.get("part")

        platform = value.get("platform").lower()
        code = value.get(code_field)

        # Overwrite the code value
        # ios does not follow code definition
        if platform == 'ios' and code == '0000000':
            code = '9991007'
        # sdk defined same number for different actual code
        elif platform == 'android' and code == '0012001':
            code = '0012247'
        elif platform == 'android' and code == '0012002':
            code = '0012248'

        return self.extract_error_code(code, part=part)

    def is_dummy_email(self, email: str) -> bool:
        if email is not None and len(email.strip()) > 0:
            if self.DUMMY_EMAIL.match(email.lower()):
                return True

        return False

    def is_dummy_device(self, serial_number: str) -> bool:
        if serial_number is not None and len(serial_number.strip()) > 0:
            if self.DUMMY_DEVICE.match(serial_number.upper()):
                return True

        return False

    def extract_ate_id(self, ate_id: str) -> any:
        match = self.ATE_PATTERN_STANDARD_1.match(ate_id)
        ate_category = None
        ate_station = None
        factory = None
        station_number = -1
        city = None

        if match:
            item = match.groups()
            ate_category = item[0]
            ate_station = item[2]
            factory = item[3]
            station_number = item[4]
            city = item[5]

        if not match:
            match = self.ATE_PATTERN_STANDARD_3.match(ate_id)

            if match:
                item = match.groups()
                ate_category = item[0]
                ate_station = item[2]
                station_number = item[3]
                factory = item[4]
                city = item[5]

        if ("2.5" in ate_id) and (not match):
            ate_station = "2.5"
            match = self.ATE_PATTERN_2_5_1.match(ate_id)

            if match:
                item = match.groups()
                ate_category = item[0]
                factory = item[1]
                station_number = item[2]
            else:
                match = self.ATE_PATTERN_2_5_2.match(ate_id)

                if match:
                    item = match.groups()
                    ate_category = item[0]
                    factory = item[1]
                    station_number = item[2]
                    city = item[3]

        if not match:
            match = self.ATE_PATTERN_STANDARD_2.match(ate_id)

            if match:
                item = match.groups()
                ate_category = item[0]
                ate_station = item[1]
                factory = item[2]
                station_number = item[3]

        if factory:
            factory = factory.upper()

            if "VN" in factory or "VIETNAM" in factory:
                factory = "VN"
                city = "HCMC"

        return (ate_category, ate_station, factory, int(station_number
                                                        or -1), city)

    def crc32_to_app_name(self, value: any) -> str:
        # List CRC32 of node name: https://go.fossil.com/hw-log-sumup
        # Convert raw to this mapping: https://s.duyet.net/r/FgAR
        mapping = {
            "10918": "timerApp",
            "11757": "wellnessApp",
            "13134": "commuteApp",
            "14388": "chargerConnected",
            "15574": "workoutApp",
            "18444": "musicApp",
            "18488": "lowBattery",
            "20509": "weatherSSE",
            "27543": "settingsApp",
            "296": "dateSSE",
            "29936": "assistantApp",
            "30127": "timerService",
            "34161": "stopwatchApp",
            "35047": "hrSSE",
            "37511": "inactivityNudgeApp",
            "37953": "weatherApp",
            "40951": "terminalApp",
            "41250": "diagnosticsApp",
            "41942": "master",
            "42130": "watchFace",
            "44418": "caloriesSSE",
            "45267": "notifications",
            "48528": "urgentNotifications",
            "48610": "handCalib",
            "49452": "timeZone2SSE",
            "49476": "chanceOfRainSSE",
            "49740": "shipMode",
            "4989": "notificationsPanelApp",
            "59159": "stepsSSE",
            "60410": "buddyChallengeApp",
            "61260": "activeMinutesSSE",
            "62904": "resetScreen",
            "63250": "commuteSSE",
            "64249": "incomingCall",
        }

        return mapping.get(str(value))

    def extract_timestamp_from_minute_data(self, minute_data: dict) -> int:
        timestamp = minute_data.get("startTime", None)

        if not timestamp:
            timestamp = minute_data.get("timestamp", None)

        try:
            return int(timestamp)
        except Exception:
            # logger.debug(
            #     f'extract_timestamp_from_minute_data: cannot cast {timestamp} to int'
            # )
            return None

    def extract_software_reset_cause_infomation(self, record: dict) -> any:
        software_reset_cause_key = "Unknown"
        information_key_1 = ''
        information_col_1 = ''
        information_col_1_hex = ''
        information_key_2 = ''
        information_col_2 = ''
        information_col_2_hex = ''
        primary_code = record['parsed_hardware_log']['primary_code']

        if primary_code == 2:
            software_reset_cause_key = "Wicentric_Assert"
            information_key_1 = 'line_number'
            information_col_1 = record['parsed_hardware_log']['data'][
                'line_number']
        elif primary_code == 5:
            software_reset_cause_key = "Hard_Fault"
            information_key_1 = 'last_pc_address'
            information_col_1 = record['parsed_hardware_log']['data'][
                'last_pc_address']
            information_col_1_hex = record['parsed_hardware_log']['data'][
                'last_pc_address_hex']
            information_key_2 = 'last_lr_address'
            information_col_2 = record['parsed_hardware_log']['data'][
                'last_lr_address']
            information_col_2_hex = record['parsed_hardware_log']['data'][
                'last_lr_address_hex']
        elif primary_code == 6:
            software_reset_cause_key = "Bus_Fault"
            information_key_1 = 'last_pc_address'
            information_col_1 = record['parsed_hardware_log']['data'][
                'last_pc_address']
            information_col_1_hex = record['parsed_hardware_log']['data'][
                'last_pc_address_hex']
            information_key_2 = 'last_lr_address'
            information_col_2 = record['parsed_hardware_log']['data'][
                'last_lr_address']
            information_col_2_hex = record['parsed_hardware_log']['data'][
                'last_lr_address_hex']
        elif primary_code == 7:
            software_reset_cause_key = "Usage_Fault"
            information_key_1 = 'last_pc_address'
            information_col_1 = record['parsed_hardware_log']['data'][
                'last_pc_address']
            information_col_1_hex = record['parsed_hardware_log']['data'][
                'last_pc_address_hex']
            information_key_2 = 'last_lr_address'
            information_col_2 = record['parsed_hardware_log']['data'][
                'last_lr_address']
            information_col_2_hex = record['parsed_hardware_log']['data'][
                'last_lr_address_hex']
        elif primary_code == 12:
            software_reset_cause_key = "Comm_Lockup_Error"
        elif primary_code == 21:
            software_reset_cause_key = "Watch_dog"
            information_key_1 = 'last_pc_address'
            information_col_1 = record['parsed_hardware_log']['data'][
                'last_pc_address']
            information_col_1_hex = record['parsed_hardware_log']['data'][
                'last_pc_address_hex']
            information_key_2 = 'last_lr_address'
            information_col_2 = record['parsed_hardware_log']['data'][
                'last_lr_address']
            information_col_2_hex = record['parsed_hardware_log']['data'][
                'data']['last_lr_address_hex']
        elif primary_code == 23:
            software_reset_cause_key = "MSF_Assert"
            information_key_1 = 'line_number'
            information_col_1 = record['parsed_hardware_log']['data'][
                'line_number']
            information_key_2 = 'file_name'
            information_col_2 = record['parsed_hardware_log']['data'][
                'file_name']
        elif primary_code == 101:
            software_reset_cause_key = "Stack_Over_Flow"
            information_key_1 = 'thread_id'
            information_col_1 = record['parsed_hardware_log']['data'][
                'thread_id']
        elif primary_code == 144 and record['parsed_hardware_log'].get(
                'secondary_code') == 138:
            software_reset_cause_key = "Disconnect_Failed"
        return (software_reset_cause_key, information_key_1,
                str(information_col_1), information_col_1_hex,
                information_key_2, str(information_col_2),
                information_col_2_hex)

    def generate_geo_id(self, value: dict) -> str:
        str_geo_id: str = (value["continent"] + value["country"] +
                        value["region"] + value["city"] +
                        value["sub_continent"])

        hashed = hashlib.md5(str_geo_id.encode('utf-8'))
        return hashed.hexdigest()

    def extract_device_type_from_sn_prefix(self, sn_prefix: str) -> str:
        # https://docs.google.com/spreadsheets/d/1sgD4AgsjWLeb2KEEeXnnXomxf1d1B0jex4H4_OHRPOE/edit#gid=0
        pattern = re.compile("[A-Z0-9]+")
        if sn_prefix and pattern.fullmatch(sn_prefix):
            tracker_sn_prefixs = ["C0D101", "C0D102", "C0D201", "C0D202",
                                "C0K101", "C0K102", "C0K103", "C0K104", "C0K105", "C0K106", "C0K107", "C0K108",
                                "C0K109", "C0K201", "C0K202", "C0K301", "C0K302", "C0K303", "C0K304", "C0K305",
                                "C0K306", "C0K307", "C0K308", "C0K309",
                                "C0M101", "C0M102", "C0M103", "C0M104", "C0M105", "C0M106", "C0M107", "C0M201",
                                "C0M202", "C0M203", "C0M204", "C0M205", "C0M301", "C0M302", "C0M303", "C0M304",
                                "C0M305", "C0M401", "C0M402", "C0M403", "C0M501", "C0M502", "C0M503",
                                "C0S101", "C0S102"]

            if sn_prefix in tracker_sn_prefixs:
                return 'Tracker'
            elif sn_prefix.startswith('K') or sn_prefix.startswith('C'):
                return 'WearOS'
            else:
                return 'Hybrid'
        else:
            return 'Hybrid'

field_transformer = FieldTransformer()