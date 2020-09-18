# standard library imports
import pandas as pd
from sqlalchemy import create_engine
from dateutil.parser import isoparse
import sqlalchemy as sal

# %% dictionaries to code the identifier and status columns
record_identifier = {
    "C": 0,  # closest approach to a coast, not followed by a landfall
    "G": 1,  # genesis
    "I": 2,  # an intensity peak in terms of both pressure and wind
    "L": 3,  # landfall
    "P": 4,  # minimum central pressure
    "R": 5,  # additional detail on intensity of cyclone when rapid changes are underway
    "S": 6,  # change in status of the system
    "T": 7,  # provides additional detail on the track (position) of the cyclone
    "W": 8,  # maximum sustained wind speed
    " ": pd.NA  # missing value
}

status = {
    "TD": 0,  # tropical cyclone of tropical depression intensity (<34 knots)
    "TS": 1,  # tropical cyclone of tropical storm intensity (34-63 knots)
    "HU": 2,  # tropical cyclone of hurricane intensity (>= 64 knots)
    "EX": 3,  # extratropical cyclone of any intensity
    "SD": 4,  # subtropical cyclone of subtropical depression intensity (<34 knots)
    "SS": 5,  # subtropical cyclone of subtropical storm intensity (>= 34 knots)
    "LO": 6,  # a low that is neither a tropical cyclone, a subtropical cyclone, nor an extrantropical cyclone
    "WV": 7,  # a tropical wave
    "DB": 8,  # disturbance of any intensity
    "  ": pd.NA,  # missing value
    "ET": pd.NA,  # invalid value found in pacific dataset, maybe this means EX?
    "TY": pd.NA,  # invalid value found in pacific dataset
    "ST": pd.NA,  # invalid value found in pacific dataset. maybe this means SS?
    "PT": pd.NA  # invalid value found in pacific dataset
}


def process_file(file_path):
    file_headers = []
    file_data = []
    file = open(file_path, 'r')
    for row in file:
        row_split = row.split(",")
        if len(row_split) == 4:
            # header row
            file_headers.append([
                row_split[0],  # event id
                row_split[0][0:2],  # basin
                row_split[0][2:4],  # storm number
                row_split[0][4:8],  # year
                row_split[1].lstrip(" "),  # name
                int(row_split[2])  # number of data points
            ])
        else:
            # data row
            file_data.append([
                file_headers[-1][0],  # event id. Need this to link back to header table.
                row_split[0][0:4],  # year
                row_split[0][4:6],  # month
                row_split[0][6:8],  # day
                row_split[1].lstrip(" ")[0:2],  # hours in UTC
                row_split[1][-2:],  # minutes in UTC
                row_split[2][-1],  # record identifier
                row_split[3][-2:],  # storm status code
                float(row_split[4][:-1]) * (-1 if row_split[4][-1] == "S" else 1),  # Latitude
                float(row_split[5][:-1]) * (-1 if row_split[5][-1] == "W" else 1),  # Longitude
                int(row_split[6].lstrip(" ")),  # maximum sustained wind (knots)
                int(row_split[7].lstrip(" ")),  # minimum central pressure
                int(row_split[8].lstrip(" ")),  # 34 kt wind radii max extent in N-E quadrant in nautical miles
                int(row_split[9].lstrip(" ")),  # 34 kt wind radii max extent in S-E quadrant in nautical miles
                int(row_split[10].lstrip(" ")),  # 34 kt wind radii max extent in S-W quadrant in nautical miles
                int(row_split[11].lstrip(" ")),  # 34 kt wind radii max extent in N-W quadrant in nautical miles
                int(row_split[12].lstrip(" ")),  # 50 kt wind radii max extent in N-E quadrant in nautical miles
                int(row_split[13].lstrip(" ")),  # 50 kt wind radii max extent in S-E quadrant in nautical miles
                int(row_split[14].lstrip(" ")),  # 50 kt wind radii max extent in S-W quadrant in nautical miles
                int(row_split[15].lstrip(" ")),  # 50 kt wind radii max extent in N-W quadrant in nautical miles
                int(row_split[16].lstrip(" ")),  # 64 kt wind radii max extent in N-E quadrant in nautical miles
                int(row_split[17].lstrip(" ")),  # 64 kt wind radii max extent in S-E quadrant in nautical miles
                int(row_split[18].lstrip(" ")),  # 64 kt wind radii max extent in S-W quadrant in nautical miles
                int(row_split[19].lstrip(" ")),  # 64 kt wind radii max extent in N-W quadrant in nautical miles
            ])
    file.close()
    return file_headers, file_data


def clean_data(events, points):
    events = pd.DataFrame(
        events,
        columns=[
            "event_id",
            "basin",
            "storm_num",
            "year",
            "name",
            "num_points"
        ]
    )
    points = pd.DataFrame(
        points,
        columns=[
            "event_id",
            "year",
            "month",
            "day",
            "hours_UTC",
            "minutes_UTC",
            "identifier",
            "status",
            "latitude",
            "longitude",
            "max_wind_knots",
            "min_pressure_mb",
            "ne_34kt_radii_max_nm",
            "se_34kt_radii_max_nm",
            "sw_34kt_radii_max_nm",
            "nw_34kt_radii_max_nm",
            "ne_50kt_radii_max_nm",
            "se_50kt_radii_max_nm",
            "sw_50kt_radii_max_nm",
            "nw_50kt_radii_max_nm",
            "ne_64kt_radii_max_nm",
            "se_64kt_radii_max_nm",
            "sw_64kt_radii_max_nm",
            "nw_64kt_radii_max_nm"
        ]
    )
    points["location"] = "POINT(" + points["longitude"].astype(str) + " " + points["latitude"].astype(str) + ")"
    points["point_time"] = points["year"] + "-" + points["month"] + "-" + points["day"] + "T" \
        + points["hours_UTC"] + ":" + points["minutes_UTC"] + ":00.000Z"
    points["point_time"] = points["point_time"].apply(isoparse)
    points["path"] = points["longitude"].astype(str) + " " + points["latitude"].astype(str) + " " \
        + points["max_wind_knots"].astype(str).replace("-99", "NULL") + " " \
        + points["min_pressure_mb"].astype(str).replace("-999", "NULL")
    points.drop(
        ["year", "month", "day", "hours_UTC", "minutes_UTC", "latitude", "longitude"],
        axis=1,
        inplace=True
    )
    points.replace(
        {
            "identifier": record_identifier,
            "status": status,
            "max_wind_knots": {-99: pd.NA},
            "min_pressure_mb": {-999: pd.NA},
            "ne_34kt_radii_max_nm": {-999: pd.NA},
            "se_34kt_radii_max_nm": {-999: pd.NA},
            "sw_34kt_radii_max_nm": {-999: pd.NA},
            "nw_34kt_radii_max_nm": {-999: pd.NA},
            "ne_50kt_radii_max_nm": {-999: pd.NA},
            "se_50kt_radii_max_nm": {-999: pd.NA},
            "sw_50kt_radii_max_nm": {-999: pd.NA},
            "nw_50kt_radii_max_nm": {-999: pd.NA},
            "ne_64kt_radii_max_nm": {-999: pd.NA},
            "se_64kt_radii_max_nm": {-999: pd.NA},
            "sw_64kt_radii_max_nm": {-999: pd.NA},
            "nw_64kt_radii_max_nm": {-999: pd.NA}
        },
        inplace=True
    )
    events["start_time"] = points.groupby("event_id", sort=False).first()["point_time"].values
    events["path"] = points.loc[:, ["event_id", "path"]].groupby("event_id", sort=False)["path"].apply(",".join).values
    events.loc[events["num_points"] == 1, "path"] = "POINT(" + events.loc[events["num_points"] == 1, "path"] + ")"
    events.loc[events["num_points"] > 1, "path"] = "LINESTRING(" + events.loc[events["num_points"] > 1, "path"] + ")"
    events.drop(["storm_num", "num_points", "year"], axis=1, inplace=True)
    points.drop(["path"], axis=1, inplace=True)
    return events, points


if __name__ == "__main__":
    atlantic_path = "resources//hurdat2-1851-2019-052520.txt"
    pacific_path = "resources//hurdat2-nepac-1949-2019-042320.txt"
    #%% process Atlantic basin HURDAT2 file
    atlantic_headers, atlantic_data = process_file(atlantic_path)
    pacific_headers, pacific_data = process_file(pacific_path)
    #%% clean data
    atlantic_headers, atlantic_data = clean_data(atlantic_headers, atlantic_data)
    pacific_headers, pacific_data = clean_data(pacific_headers, pacific_data)
    #%% load data into sql server
    engine = create_engine(
        "mssql+pyodbc://bhsiairsql03/FinancialModuleTesting?Driver=ODBC Driver 17 for SQL Server?Trusted_Connection=yes",
        fast_executemany=True
    )
    conn = engine.connect()
    table_types = {
        "event_id": sal.types.NCHAR(length=8),
        "identifier": sal.types.INTEGER(),
        "status": sal.types.INTEGER(),
        "max_wind_knots": sal.types.INTEGER(),
        "min_pressure_mb": sal.types.INTEGER(),
        "ne_34kt_radii_max_nm": sal.types.INTEGER(),
        "se_34kt_radii_max_nm": sal.types.INTEGER(),
        "sw_34kt_radii_max_nm": sal.types.INTEGER(),
        "nw_34kt_radii_max_nm": sal.types.INTEGER(),
        "ne_50kt_radii_max_nm": sal.types.INTEGER(),
        "se_50kt_radii_max_nm": sal.types.INTEGER(),
        "sw_50kt_radii_max_nm": sal.types.INTEGER(),
        "nw_50kt_radii_max_nm": sal.types.INTEGER(),
        "ne_64kt_radii_max_nm": sal.types.INTEGER(),
        "se_64kt_radii_max_nm": sal.types.INTEGER(),
        "sw_64kt_radii_max_nm": sal.types.INTEGER(),
        "nw_64kt_radii_max_nm": sal.types.INTEGER(),
        "location": sal.types.NVARCHAR(length=100),
        "point_time": sal.DateTime()
    }
    atlantic_data.to_sql("Historical_HU_points", con=engine, if_exists="replace", dtype=table_types, index=False)
    pacific_data.to_sql("Historical_HU_points", con=engine, if_exists="append", dtype=table_types, index=False)
    table_types = {
        "event_id": sal.types.NCHAR(length=8),
        "basin": sal.types.NVARCHAR(length=2),
        "name": sal.types.NVARCHAR(length=40),
        "start_time": sal.DateTime(),
        "path": sal.types.NVARCHAR()
    }
    atlantic_headers.to_sql("Historical_HU", con=engine, if_exists="replace", dtype=table_types, index=False)
    pacific_headers.to_sql("Historical_HU", con=engine, if_exists="append", dtype=table_types, index=False)

    #%% create keys, indexes, and geo datatypes
    sql = (
        "ALTER TABLE Historical_HU ALTER column event_id nchar(8) not null; "
        "ALTER TABLE Historical_HU_points ADD point_id int NOT NULL IDENTITY;"
    )
    conn.execute(sql)
    sql = (
        "ALTER TABLE Historical_HU "
        "ADD CONSTRAINT PK_Historical_HU_event_id PRIMARY KEY CLUSTERED (event_id), path_geo geography;"
        "ALTER TABLE Historical_HU_points "
        "ADD CONSTRAINT PK_Historical_HU_point_id PRIMARY KEY CLUSTERED (point_id), "
        "FOREIGN KEY (event_id) REFERENCES Historical_HU(event_id), location_geo geography;"

    )
    conn.execute(sql)
    sql = (
        "UPDATE Historical_HU SET path_geo = geography::STGeomFromText(path, 4326);"
        "CREATE SPATIAL INDEX Historical_HU_path ON Historical_HU (path_geo) USING GEOGRAPHY_AUTO_GRID;"
        "UPDATE Historical_HU_points SET location_geo = geography::STGeomFromText(location, 4326);"
        "CREATE SPATIAL INDEX Historical_HU_point on Historical_HU_points (location_geo) USING GEOGRAPHY_AUTO_GRID;"
    )
    conn.execute(sql)
    conn.close()


