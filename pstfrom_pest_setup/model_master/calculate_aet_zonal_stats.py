import xarray as xr
import rioxarray as rio
import xrspatial as xrs
from rasterio import features
import pandas as pd
import geopandas as gp
import datetime as dt
import argparse
import numpy as np
from stats_functions import summarize_array_values, calculate_zonal_statistics

num_zone_chars = 3

grid_filename = 'actual_et__2016-01-01_to_2018-12-31__688_by_620.nc'
#zone_filename = 'observation_watersheds_zones_1000m.asc'
surface_water_basins_shp = '../ALL_BASINS_FOR_OBS_GRID__EPSG_4269.shp'
variable_name = 'actual_et'

ds = xr.open_dataset(grid_filename, decode_cf=True, decode_coords=True, engine='netcdf4')
#mask_dataarray = rio.open_rasterio(zone_filename)

surface_water_basins_gdf = gp.read_file(surface_water_basins_shp).to_crs(epsg=5070)
# rasterio is not wired to deal directly with geopandas objects; we must extract the geometry from the geopandas object
# see https://pygis.io/docs/e_raster_rasterize.html
#geom = [shapes for shapes in surface_water_basins_gdf.geometry]
geom = surface_water_basins_gdf[['geometry','BASIN_INDX']].values.tolist()
zones = features.rasterize(geom, out_shape=ds.actual_et[0,:,:].shape, transform=ds.rio.transform(), fill=-9999)
# zonal_stats requires the mask to be a a dataarray; here we create a dataarray with the same dimensions as the seasonal_da,
# copying the values of the mask into the dataarray
zones_da = ds.actual_et[0,:,:].copy()
zones_da.data = zones.astype(np.integer)

# return 'n' grids of summed daily gridded values with 'n' equal to the number of subsequent quarters in the input dataset
xarray_dataarray = ds[variable_name].resample(time="QS-DEC").reduce(np.sum, dim="time")

summary_type = 'quarterly_sum'

dims = list(xarray_dataarray.dims)

# for i in range(xarray_dataarray.shape[0]):
#     df = xrs.zonal.stats(zones=zones_da, values=xarray_dataarray[i,:,:])
#     if 'time' in dims:
#         t = xarray_dataarray['time'][i].values
#         year=pd.to_datetime(t).year
#         month=pd.to_datetime(t).month
#     elif 'month' in dims:
#         year = np.nan
#         month = xarray_dataarray['month'][i].values
#     match summary_type:
#         case 'monthly_sum' | 'monthly_mean':
#             df['month'] = month
#             df['year'] = year
#             df['date'] = dt.datetime.strptime(f"{year}-{month}-15", "%Y-%m-%d")
#             df['water_year'] = df['year'].where(df['month'] < 10, df['year'] + 1)
#         case 'quarterly_sum':
#             df['month'] = month
#             df['year'] = year
#             df['date'] = dt.datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
#             match month:
#                 case 12:
#                     season = 'DJF'
#                 case 3:
#                     season = 'MAM'
#                 case 6:
#                     season = 'JJA'
#                 case 9:
#                     season = 'SON'
#             df['season'] = season
#         case 'mean_monthly_sum' | 'mean_monthly_mean':
#             # no 'year' or 'date' element, since this should summarize all values for a given month
#             df['month'] = month
#         case 'annual_sum' | 'annual_mean':
#             df['month'] = 6
#             df['year'] = year
#             df['date'] = dt.datetime.strptime(f"{year}-06-01", "%Y-%m-%d")
#             df['water_year'] = df['year'].where(df['month'] < 10, df['year'] + 1)
#         case 'mean_annual_sum' | 'mean_annual_mean':
#             pass
#             # for mean annual calculations there is no meaningful timestamp, year, or month value
#         case _:
#             print(f"unknown calculation_type '{summary_type}'")
#             exit(1) 

#     if i == 0:
#         zonal_stats = df.copy()
#     else:
#         zonal_stats = pd.concat([zonal_stats, df])

# # convert zone labels to string, prepend '0' if desired
# #zonal_stats['zone'] = fix_zone_labels(zonal_stats['zone'], num_zone_chars)
# zonal_stats['zone'] = zonal_stats['zone'].apply(str)
# if num_zone_chars is not None:
#     zonal_stats['zone'] = zonal_stats['zone'].apply(lambda x: f"{x:0>{num_zone_chars}}")

# zs = zonal_stats.query("zone>='0'")

results_df = calculate_zonal_statistics(xarray_dataarray=xarray_dataarray, mask_dataarray=zones_da, summary_type='quarterly_sum', num_zone_chars=2)
results_df = results_df.query("zone != '-9999'")
results_df.to_csv('simulated_actual_et.csv')
