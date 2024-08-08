GRID 620 688 -125045.0 2246285.0 1000.0
BASE_PROJECTION_DEFINITION +proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23.0 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs

# MODULE SPECIFICATION
-------------------------

INTERCEPTION_METHOD              BUCKET
EVAPOTRANSPIRATION_METHOD        HARGREAVES
RUNOFF_METHOD                    CURVE_NUMBER
SOIL_MOISTURE_METHOD             THORNTHWAITE-MATHER
PRECIPITATION_METHOD             TABULAR
FOG_METHOD                       NONE
FLOW_ROUTING_METHOD              NONE
IRRIGATION_METHOD                NONE
CROP_COEFFICIENT_METHOD          NONE
GROWING_DEGREE_DAY_METHOD        MODIFIED_GROWING_DEGREE-DAY
ROOTING_DEPTH_METHOD             STATIC
DIRECT_NET_INFILTRATION_METHOD   NONE
DIRECT_SOIL_MOISTURE_METHOD      NONE
SOIL_STORAGE_MAX_METHOD          TABLE


# Gridded input files:
# -----------------------------------------------
AVAILABLE_WATER_CONTENT ARC_GRID AWS_grid_MN__1000m.asc
AVAILABLE_WATER_CONTENT_PROJECTION_DEFINITION +proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23.0 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs

LAND_USE ARC_GRID NLCD1992__1000m.asc
LAND_USE_PROJECTION_DEFINITION +proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23.0 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs

HYDROLOGIC_SOILS_GROUP ARC_GRID HSG_grid_MN__1000m.asc
HYDROLOGIC_SOILS_GROUP_PROJECTION_DEFINITION +proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23.0 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs

IRRIGATION_MASK CONSTANT 1.0

# Miscellaneous inputs
# ---------------------------------------------------
INITIAL_CONTINUOUS_FROZEN_GROUND_INDEX   CONSTANT  100.0
UPPER_LIMIT_CFGI 83.
LOWER_LIMIT_CFGI 55.

GROWING_SEASON 133 268 TRUE

INITIAL_PERCENT_SOIL_MOISTURE            CONSTANT  100.0
INITIAL_SNOW_COVER_STORAGE               CONSTANT   0.0

# Lookup Tables:
# -------------------------------------------
LAND_USE_LOOKUP_TABLE LU_lookup_MN_v3.txt
IRRIGATION_LOOKUP_TABLE IRR_lookup_MN_v3.txt
WEATHER_DATA_LOOKUP_TABLE Minnesota_PRISM_daily_weather__US-Ro6__col_358__row_519__x_232693__y_2415326.tsv

# Output options
#---------------
OUTPUT ENABLE snowmelt snow_storage reference_ET0
#OUTPUT DISABLE snowfall runon rainfall 
OUTPUT DISABLE soil_storage delta_soil_storage surface_storage infiltration
OUTPUT ENABLE tmax tmin gross_precip runoff_outside actual_et
OUTPUT ENABLE rejected_net_infiltration net_infiltration

# the following will dump out SWB variable values for a single grid cell;
# the locations correspond to AmeriFlux tower locations

DUMP_VARIABLES COORDS 232693 2415326 ID US-Ro6

START_DATE 01/01/2016
END_DATE 12/31/2018

