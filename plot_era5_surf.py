"""
Program to plot ERA5 wind speed for a given date.

    Usage:  python plot_era5_surf.py YYYYMMDDHH
    Output: YYYYMMDDHH.png

ERA5 surface has U and V components at 10m and 100m levels 
Module and Direction can be easily calculated
GRIB file downloaded from Copernicus Data Store

"""

import numpy  as np
import xarray as xr
import dask.array as da
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
import cdsapi
import sys
from datetime import datetime

attributes_vars = {
    
    'M': {'description': 'Wind Speed',
          'long_name': 'Wind speed',
          'units': 'm/s'},
    
    'Dir': {'description': 'Wind Direction',
            'long_name': 'Wind direction',
            'units': 'degrees'},
}

def wind_speed(dataset):
    """
    Calculates the wind speed from U and V
    """
    u = dataset['u100']
    v = dataset['v100']
    m = xr.apply_ufunc(da.sqrt, u**2 + v**2, dask='allowed').rename('M')
    m.attrs = attributes_vars['M']
    return m


def direction(dataset):
    """
    Calculates wind direction from U and V
    """
    u = dataset['u100']
    v = dataset['v100']
    radians = xr.apply_ufunc(da.arctan2, u, v, dask='allowed')
    d = (radians * 180 / math.pi + 180).rename('Dir')
    d.attrs = attributes_vars['Dir']
    return d


def plot_map(dataset,outfile):
    """
    Plots a map with right axes, coastlines and colorbar 
    """
    fig = plt.figure(figsize=(11,8.5))

    # Set the axes using the specified map projection
    ax=plt.axes(projection=ccrs.PlateCarree())

    # Add coastlines
    ax.coastlines()
    ws=dataset.plot(add_colorbar=False)

    # Define the xticks for longitude
    ax.set_xticks(np.arange(-180,181,60), crs=ccrs.PlateCarree())
    lon_formatter = cticker.LongitudeFormatter()
    ax.xaxis.set_major_formatter(lon_formatter)

    # Define the yticks for latitude
    ax.set_yticks(np.arange(-90,91,30), crs=ccrs.PlateCarree())
    lat_formatter = cticker.LatitudeFormatter()
    ax.yaxis.set_major_formatter(lat_formatter)

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.1, 0.15, 0.8, 0.02])
    cbar=fig.colorbar(ws,cax=cbar_ax,orientation='horizontal')

    # Save image file
    fig.savefig(outfile)

    return

def download_era5(dyear,dmonth,dday,dhour,gribfile):
    """
    Downloads ERA5 surface from CDS
    """
    c = cdsapi.Client()

    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'variable': ['100m_u_component_of_wind', '100m_v_component_of_wind', '10m_u_component_of_wind',
            '10m_v_component_of_wind',],
            'year': dyear,
            'month': dmonth,
            'day': dday,
            'time': dhour,
            'format': 'grib',
         },
        gribfile+'.grib')
    
    return



era5date = sys.argv[1] # Reads date from console
my_date = datetime.strptime(era5date, "%Y%m%d%H")
date = my_date.date()
time = my_date.time()
download_era5(date.year,date.month,date.day,time.hour,era5date)
era5 = xr.open_dataset(era5date+'.grib', engine='cfgrib')
plot_map(wind_speed(era5),era5date)


