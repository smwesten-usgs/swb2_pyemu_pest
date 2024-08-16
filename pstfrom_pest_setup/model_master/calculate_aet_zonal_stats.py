import xarray as xr
import rioxarray as rio
import xrspatial as xrs
import pandas as pd
import datetime as dt
import argparse
import numpy as np

num_zone_chars = 3

grid_filename = 'actual_et__2016-01-01_to_2018-12-31__688_by_620.nc'
zone_filename = 'observation_watersheds_zones_1000m.asc'
variable_name = 'actual_et'

ds = xr.open_dataset(grid_filename, decode_cf=True, decode_coords=True, engine='netcdf4')
mask_dataarray = rio.open_rasterio(zone_filename)

# return 'n' grids of summed daily gridded values with 'n' equal to the number of subsequent quarters in the input dataset
xarray_dataarray = ds[variable_name].resample(time="QS-DEC").reduce(np.sum, dim="time")

summary_type = 'quarterly_sum'

dims = list(xarray_dataarray.dims)

for i in range(xarray_dataarray.shape[0]):
    df = xrs.zonal.stats(zones=mask_dataarray[0,:,:], values=xarray_dataarray[i,:,:])
    if 'time' in dims:
        t = xarray_dataarray['time'][i].values
        year=pd.to_datetime(t).year
        month=pd.to_datetime(t).month
    elif 'month' in dims:
        year = np.nan
        month = xarray_dataarray['month'][i].values
    match summary_type:
        case 'monthly_sum' | 'monthly_mean':
            df['month'] = month
            df['year'] = year
            df['date'] = dt.datetime.strptime(f"{year}-{month}-15", "%Y-%m-%d")
            df['water_year'] = df['year'].where(df['month'] < 10, df['year'] + 1)
        case 'quarterly_sum':
            df['month'] = month
            df['year'] = year
            df['date'] = dt.datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
            match month:
                case 12:
                    season = 'DJF'
                case 3:
                    season = 'MAM'
                case 6:
                    season = 'JJA'
                case 9:
                    season = 'SON'
            df['season'] = season
        case 'mean_monthly_sum' | 'mean_monthly_mean':
            # no 'year' or 'date' element, since this should summarize all values for a given month
            df['month'] = month
        case 'annual_sum' | 'annual_mean':
            df['month'] = 6
            df['year'] = year
            df['date'] = dt.datetime.strptime(f"{year}-06-01", "%Y-%m-%d")
            df['water_year'] = df['year'].where(df['month'] < 10, df['year'] + 1)
        case 'mean_annual_sum' | 'mean_annual_mean':
            pass
            # for mean annual calculations there is no meaningful timestamp, year, or month value
        case _:
            print(f"unknown calculation_type '{summary_type}'")
            exit(1) 

    if i == 0:
        zonal_stats = df.copy()
    else:
        zonal_stats = pd.concat([zonal_stats, df])

# convert zone labels to string, prepend '0' if desired
#zonal_stats['zone'] = fix_zone_labels(zonal_stats['zone'], num_zone_chars)
zonal_stats['zone'] = zonal_stats['zone'].apply(str)
if num_zone_chars is not None:
    zonal_stats['zone'] = zonal_stats['zone'].apply(lambda x: f"{x:0>{num_zone_chars}}")

zs = zonal_stats.query("zone>='0'")
zs.to_csv('simulated_actual_et.csv')
