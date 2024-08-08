import xarray as xr
import rioxarray as rio
import xrspatial as xrs
import pandas as pd
import datetime as dt
import argparse
import numpy as np

num_zone_chars = 3

parser = argparse.ArgumentParser(description='Read in a gridded data file and output a summary csv')
parser.add_argument('--grid_file',
                    help='file containing gridded data, as a *.nc')
parser.add_argument('--variable_name',
                    help='name of the variable to summarize, if the grid_file is a netCDF file')
parser.add_argument('--zone_file',
                    help='Arc ASCII grid containing zones by which to summarize the gridded data')

args = parser.parse_args()
grid_filename = args.grid_file
zone_filename = args.zone_file
variable_name = args.variable_name

ds = xr.open_dataset(grid_filename, decode_cf=True, decode_coords=True, engine='netcdf4')
mask_dataarray = rio.open_rasterio(zone_filename)

# return 'n/4' grids of summed daily gridded values with 'n/4' equal to the number of quarters in the input dataset
xarray_dataarray = ds[variable_name].resample(time="QS-DEC").reduce(np.sum, dim="time")


dims = list(xarray_dataarray.dims)
summary_type = 'seasonal_sum'

if ('time' in dims or 'month' in dims):
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
            case 'seasonal_sum':
                df['month'] = month
                df['year'] = year
                df['date'] = dt.datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
                df['water_year'] = df['year'].where(df['month'] < 10, df['year'] + 1)
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

        #df['zone'] = df['zone']

        if i == 0:
            zonal_stats = df.copy()
        else:
            zonal_stats = pd.concat([zonal_stats, df])

else:
    zonal_stats = xrs.zonal.stats(zones=mask_dataarray, values=xarray_dataarray)

# convert zone labels to string, prepend '0' if desired
#zonal_stats['zone'] = fix_zone_labels(zonal_stats['zone'], num_zone_chars)
zonal_stats['zone'] = zonal_stats['zone'].apply(str)
if num_zone_chars is not None:
    zonal_stats['zone'] = zonal_stats['zone'].apply(lambda x: f"{x:0>{num_zone_chars}}")
