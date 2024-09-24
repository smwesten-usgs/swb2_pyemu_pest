import xarray as xr
import rioxarray as rio
import xrspatial as xrs
import numpy as np
import pandas as pd
import datetime as dt

def summarize_array_values(xarray_dataset, variable_name, summary_type='monthly_sum', crs=None):

# todo: resample is apparently just a fancy wrapper around 'groupby'; reformulating these to groupby 'water_year' 
# should be possible

    match summary_type:

        case 'seasonal_sum':
            # return 'n/4' grids of summed daily gridded values with 'n/4' equal to the number of quarters in the input dataset
            result_dataarray = xarray_dataset[variable_name].resample(time="QS-DEC").reduce(np.sum, dim="time")

        case 'seasonal_mean':
            # return 'n/4' grids of averaged daily gridded values with 'n/4' equal to the number of quarters in the input dataset
            result_dataarray = xarray_dataset[variable_name].resample(time="QS-DEC").reduce(np.mean, dim="time")

        case 'monthly_sum':     
            # return 'n' grids of summed daily gridded values with 'n' equal to the number of months in the input dataset    
            result_dataarray = xarray_dataset[variable_name].resample(time="ME").reduce(np.sum, dim="time")

        case 'monthly_mean':
            # return 'n' grids of averaged daily gridded values with 'n' equal to the number of months in the input dataset    
            result_dataarray = xarray_dataset[variable_name].resample(time="ME").reduce(np.mean, dim="time")

        case 'mean_monthly_sum':
            # return 12 grids of summed daily gridded values; each grid represents the mean of the 
            #   sum of all January values, February values, etc.    
            result_dataarray = xarray_dataset[variable_name].resample(time="ME").reduce(np.sum, dim="time").groupby("time.month").reduce(np.mean, dim="time")

        case 'mean_monthly_mean':
            # return 12 grids of averaged daily gridded values; each grid represents the mean of the 
            #   mean of all January values, February values, etc.    
            result_dataarray = xarray_dataset[variable_name].resample(time="ME").reduce(np.mean, dim="time").groupby("time.month").reduce(np.mean, dim="time")

        case 'annual_sum':
            # return 'n' grids of summed daily gridded values, with 'n' equal to the number of years in the input dataset
            #result_dataarray = xarray_dataset[variable_name].resample(time="A").sum(dim="time")
            result_dataarray = xarray_dataset[variable_name].resample(time="YE").reduce(np.sum, dim="time")

        case 'annual_mean':
            # return 'n' grids of averaged daily gridded values, with 'n' equal to the number of years in the input dataset
            #result_dataarray = xarray_dataset[variable_name].resample(time="A").mean(dim="time")
            result_dataarray = xarray_dataset[variable_name].resample(time="YE").reduce(np.mean, dim="time")

        case 'mean_annual_sum':
            # return a single grid representing the mean of each annual summed variable amount over all years
            # ==> .sum(dim='time', skipna=True) *should* result in a resampled grid that respects NaNs; however, this oes not appear to work at the moment
            result_dataarray = xarray_dataset[variable_name].resample(time="YE").reduce(np.sum, dim='time').reduce(np.mean, dim="time")

        case 'mean_annual_mean':
            # return a single grid representing the mean of each annual mean variable amount over all years
            result_dataarray = xarray_dataset[variable_name].resample(time="YE").reduce(np.mean, dim='time').reduce(np.mean, dim="time")

        case _:
              print(f"unknown calculation_type '{summary_type}'")
              exit(1)

    if crs is not None:
        result_dataarray.rio.set_crs(crs)
        result_dataarray['crs'] = crs

    return result_dataarray

def calculate_zonal_statistics(xarray_dataarray, mask_dataarray, summary_type='none', num_zone_chars=10):
    """
    Iterate over the grids in a xarray dataarray. It is assumed that this dataarray has already been
    summarized by resampling to a monthly or annual timestep. Zonal statistics are calculated for each of the
    distinct zone numbers contained in the zone mask file.
    """

    # idea here is that if there is a time series of grids, the 'time' dimension should be present, and
    # the shape should be ('time', 'y', 'x'). if we are summarizing a 'mean annual sum' grid, no 'time'
    # dimension will be present.

    dims = list(xarray_dataarray.dims)

    if ('time' in dims or 'month' in dims):
        for i in range(xarray_dataarray.shape[0]):
            df = xrs.zonal.stats(zones=mask_dataarray, values=xarray_dataarray[i,:,:])
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
                case 'mean_monthly_sum' | 'mean_monthly_mean':
                    # no 'year' or 'date' element, since this should summarize all values for a given month
                    df['month'] = month
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

    return zonal_stats
