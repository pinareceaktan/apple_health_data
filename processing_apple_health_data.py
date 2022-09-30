import os
import sys
import pandas as pd
import numpy as np
print(sys.executable)
from datetime import datetime

data_path = os.path.join(os.getcwd(), "data")
apple_data_path = os.path.join(data_path, "apple_health_export")
base_xml_file = os.path.join(apple_data_path, "export.xml")

### Extract  data from xml into a proper df
import xml.etree.ElementTree as ET
tree = ET.parse(base_xml_file)

root = tree.getroot()
record_list = [x.attrib for x in root.iter('Record')]
record_data = pd.DataFrame(record_list)
print("{0} of rows".format(len(record_data)))

### Making this df beatiful
# * Format type column
record_data["type_type"] = None
record_data["type_type"] = record_data.apply(lambda x: "continious" if "Quantity" in x["type"] else x["type_type"], axis=1)
record_data["type_type"] = record_data.apply(lambda x: "categorical" if "Category" in x["type"] else x["type_type"], axis=1)

record_data["type"] = record_data["type"].apply(lambda x: x.replace("HKQuantityTypeIdentifier", ""))
record_data["type"] = record_data["type"].apply(lambda x: x.replace("HKCategoryTypeIdentifier", ""))

# * Format device colum
def format_device_info(device_info):
    if pd.isna(device_info):
        return {"device_HKDevice":None, "device_name": None,
                "device_manufacturer":None, "device_model": None, "device_hardware":None, "device_software":None}
    device_info = device_info.replace("<<", "").replace(">", "")
    fields = list(filter(lambda x: len(x) == 2, [x.strip().split(":") for x in device_info.split(",")]))
    return {"device_"+x:y for x, y in fields}
device_meta = pd.DataFrame([format_device_info(x) for x in record_data["device"]])
if len(device_meta) != len(record_data):
    print("Lenghths missmatch check the ambiguity, something went wrong device string")
apple_health_df = pd.concat([record_data, device_meta], axis=1)
apple_health_df = apple_health_df.drop(["device"], axis=1)

# * Filter out na valued rows
before_measurements = apple_health_df["type"].unique()
apple_health_df = apple_health_df.dropna(subset=["value"])
after_measurements = apple_health_df["type"].unique()
print(set(before_measurements).difference(set(after_measurements)))

print("{0} of rows left removing null values".format(len(apple_health_df)))

# * * Taking a look at data types and sizes so we set a threshold # of records threshold.
print(apple_health_df.groupby(["type"])["value"].count().sort_values(ascending=False))

data_catelogue = apple_health_df.groupby(["type"])["value"].count().sort_values(ascending=False).reset_index()
columns = data_catelogue[data_catelogue["value"] >= 10]["type"].values

apple_health_df = apple_health_df[apple_health_df["type"].apply(lambda x: True if x in columns else False)].copy()
print(apple_health_df.shape)

# * Dealing with the time columns
def get_time_difference(time_1, time_2, in_terms_of="h"):
    """
    :param time_1: str in such a format 2021-10-25 10:21:39 +0300
    :param time_2: str in such a format 2021-10-25 10:21:39 +0300
    :param in_terms_of: "h" for hours "d" for days
    """
    dt_time_1 = datetime.strptime(time_1, "%Y-%m-%d %H:%M:%S %z")
    dt_time_2 = datetime.strptime(time_2, "%Y-%m-%d %H:%M:%S %z")
    difference = (dt_time_1 - dt_time_2)

    if in_terms_of == "h":
        return int(np.floor(difference.seconds / 60 / 60))
    elif in_terms_of == "d":
        return int(np.floor(difference.seconds / 60 / 60 / 24))


def get_date(time_1, return_type="str"):
    """
    :param time_1: str in such a format 2021-10-25 10:21:39 +0300
    :param return_type: str or datetime
    """
    dt_time_1 = datetime.strptime(time_1, "%Y-%m-%d %H:%M:%S %z").date()
    if return_type == "str":
        return dt_time_1.strftime("%Y-%m-%d")
    else:
        dt_time_1

# Format dates
apple_health_df["start_date"] = apple_health_df["startDate"].apply(lambda x: get_date(x))
apple_health_df["end_date"] = apple_health_df["endDate"].apply(lambda x: get_date(x))

# Make value column float
apple_health_df["value"] = apple_health_df.apply(lambda x: float(x["value"]) if x["type_type"] == "continious" else x["value"],
                                                 axis=1)

### Group by and concat back

apple_health_df_agg_cont = apple_health_df[apple_health_df["type_type"] == "continious"].groupby(["end_date", "type", "type_type", "device_name"]).agg({"value": "sum",
                                                                                                  "unit": set,
                                                                                                  "sourceName": set})
print(len(apple_health_df_agg_cont))

apple_health_df_agg_cat= apple_health_df[apple_health_df["type_type"] == "categorical"].groupby(["end_date", "type", "type_type", "device_name"]).agg({"value": list,
                                                                                                                                                         "unit": set,
                                                                                                                                                         "sourceName": set})
print(len(apple_health_df_agg_cat))

apple_health_final_df = pd.concat([apple_health_df_agg_cont, apple_health_df_agg_cat]).reset_index()
len(apple_health_final_df)

### Order by date

apple_health_final_df = apple_health_final_df.sort_values(by='end_date')

### Put into a csv
apple_health_final_df.to_csv("apple_health_data_ece.csv")