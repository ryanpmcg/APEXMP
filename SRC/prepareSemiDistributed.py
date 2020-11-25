# -*- coding: utf-8 -*-
#############################################################################
############################## PROGRAM METADATA #############################
#############################################################################

# Last Updated by: Ryan McGehee
# Last Updated on: 10 November 2020
# Purpose: This script is designed to prepare the DEM, land use, soil, 
#          zipcode, slope data input files for APEXMP runs.
# Contributors: Qingyu Feng (provided seperate sets of code for each data
#               which was merged and modified from his original script). 
#               Feng combined these functions in a single script and Ryan
#               refactored and tested the final code.

#############################################################################
############################# INSTRUCTIONS TO RUN ###########################
#############################################################################

# Requirements: Place this script in the ./APEXMP/SRC directory. 
#               Requires gdal module (preferred in python 3.x version).
#               All data must be in the ./APEXMP/INPUTS directory along with
#               the following:
#
# Files or directories (names without extensions are directories):    
# --working directory
#               --INPUTS
#                         --ssurgo2apex.csv      (soil database, provided)
#                         --usstctyziplatlong2sql.txt (zipcode database, provided)
#                         --studyarea 
#                            --studyarea.shp       (studyarea shapefiles, needed)
#                         --county
#                             --stcty1.shp        (all of the counties, provided) 
#                             --all               (includes all of the counties' shapefiles, provided)
#                         --dem
#                             --asc               (final maps for each county in study area)
#                             --indi              (intermediate maps, will be deleted at the end)
#                             --original 
#                                    --alldem.tif (DEM for continental US, provided)
#                         --landuse
#                             --asc               (final maps for each county in study area)
#                             --indi              (intermediate maps, will be deleted at the end)
#                             --original 
#                                    --alllanduse.tif (cropland map for continental US, provided)
#                         --soil
#                             --asc               (final maps for each county in study area)
#                             --indi              (intermediate maps, will be deleted at the end)
#                             --original 
#                                    --allsoil.tif (soil group map for continental US, provided)
#                         --zipcode
#                             --asc               (final maps for each county in study area)
#                             --indi              (intermediate maps, will be deleted at the end)
#                             --original 
#                                    --zipall.tif (zip code map for continental US, provided)
#                         --slope               
#                             --asc               (final maps for each county in study area)
#                             --indi              (intermediate maps, will be deleted at the end)
#
# Data requirement: All of the origianl datasets need to have the same cell
# size, origin, extent, and projection. 


##############################################################################
############################## IMPORT LIBRARIES ##############################
##############################################################################

import os, glob, subprocess, sys, fiona
from osgeo import ogr


#############################################################################
############################## GLOBAL VARIABLES #############################
#############################################################################

workdir = sys.argv[1]
delete = sys.argv[2]
verb = sys.argv[3]
ubmp = sys.argv[4]
bmps = sys.argv[5]
devm = sys.argv[6]
nworkers = sys.argv[7]


#############################################################################
################################ RUN PROGRAM ################################
#############################################################################

# Announce program start
if (verb == str(1)):
    print("\n")
    print("Preparing inputs for every grid cell.")
    print("\n")

# Define directories and input file paths
fd_gismain = workdir + "\\INPUTS"
fd_cty = "county"
fd_cty_all = "allcty"
fd_studyarea = "studyarea"
fd_dem = "dem"
fd_lu = "landuse"
fd_sol = "soil"
fd_zip = "zipcode"
fd_slp = "slope"
fd_data = "original"
indi = "indi"
asc = "asc"

fn_studyarea = os.path.join(fd_gismain, fd_studyarea, "studyarea.shp")
fd_ctyshp = os.path.join(fd_gismain, fd_cty, fd_cty_all)
fn_allctyshp = os.path.join(fd_gismain, fd_cty, "stcty1.shp")
fn_studyareactyshp = os.path.join(fd_gismain, fd_studyarea, "studyareacty.shp") 
fn_studyareactylist = os.path.join(fd_gismain, fd_studyarea, "studyareacty.txt") 

fn_dem = os.path.join(fd_gismain, fd_dem, fd_data, "alldem1.tif")
fn_lu = os.path.join(fd_gismain, fd_lu, fd_data, "alllanduse.tif")
fn_sol = os.path.join(fd_gismain, fd_sol, fd_data, "allsoil.tif")
fn_zip = os.path.join(fd_gismain, fd_zip, fd_data, "zipall.tif")

fd_fdemindi = os.path.join(fd_gismain, fd_dem, indi)
fd_fdemindiasc = os.path.join(fd_gismain, fd_dem, asc)
fd_fluindi = os.path.join(fd_gismain, fd_lu, indi)
fd_fluindiasc = os.path.join(fd_gismain, fd_lu, asc)
fd_fsolindi = os.path.join(fd_gismain, fd_sol, indi)
fd_fsolindiasc = os.path.join(fd_gismain, fd_sol, asc)
fd_fzipindi = os.path.join(fd_gismain, fd_zip, indi)
fd_fzipindiasc = os.path.join(fd_gismain, fd_zip, asc)
fd_fslpindi = os.path.join(fd_gismain, fd_slp, indi)
fd_fslpindiasc = os.path.join(fd_gismain, fd_slp, asc)

# Delete existing files

# Get studyarea county list by studyarea map and all counties shapefile 
## Input
driverName = "ESRI Shapefile"
driver = ogr.GetDriverByName(driverName)
inDataSource = driver.Open(fn_allctyshp, 0)
inLayer = inDataSource.GetLayer()
## Clip
inClipSource = driver.Open(fn_studyarea, 0)
inClipLayer = inClipSource.GetLayer()
## Clipped Shapefile 
outDataSource = driver.CreateDataSource(fn_studyareactyshp)
outLayer = outDataSource.CreateLayer('FINAL', geom_type=ogr.wkbMultiPolygon)
ogr.Layer.Clip(inLayer, inClipLayer, outLayer)
inDataSource.Destroy()
inClipSource.Destroy()
outDataSource.Destroy()

with fiona.open(fn_studyareactyshp) as source:
    meta = source.meta
    for f in source:
        outfile = os.path.join(fd_ctyshp, "%s.shp" % f['properties']['stcty_1'])
        try:
            with fiona.open(outfile, 'a', **meta) as sink:
                sink.write(f)
        except:
            with fiona.open(outfile, 'w', **meta) as sink:
                sink.write(f)

# Also get all counties from the county directory
datasets = glob.glob("%s/*.shp" %(fd_ctyshp))

# Pick up counties in the studyarea county list
# Modify the name to include paths and process the data for each county in the county directory

for didx in range(len(datasets)):
 
    st_cty = os.path.split(datasets[didx])[-1][:-4]
    #if st_cty in studyareactylist:
    inctyshp = datasets[didx]

    outdemras = os.path.join(fd_fdemindi, "dem%s.tif" %(st_cty))
    outluras = os.path.join(fd_fluindi, "lu%s.tif" %(st_cty))
    outsolras = os.path.join(fd_fsolindi, "sol%s.tif" %(st_cty))
    outzipras = os.path.join(fd_fzipindi, "zip%s.tif" %(st_cty))
    outfel = os.path.join(fd_gismain, fd_slp, indi, "fel%s.tif" %(st_cty))
    outp = os.path.join(fd_gismain, fd_slp, indi, "p%s.tif" %(st_cty))
    outsd8 = os.path.join(fd_gismain, fd_slp, indi, "sd8%s.tif" %(st_cty))

    outdemasc = os.path.join(fd_fdemindiasc, "dem%s.asc" %(st_cty))
    outluasc = os.path.join(fd_fluindiasc, "lu%s.asc" %(st_cty))
    outsolasc = os.path.join(fd_fsolindiasc, "sol%s.asc" %(st_cty))
    outzipasc = os.path.join(fd_fzipindiasc, "zip%s.asc" %(st_cty))
    outslpasc = os.path.join(fd_fslpindiasc, "slp%s.asc" %(st_cty))

    # Build raster masking commands
    cmd1 = 'gdalwarp -dstnodata 0 -cutline %s -crop_to_cutline %s %s' % (inctyshp, fn_dem, outdemras)
    cmd2 = 'gdalwarp -dstnodata 0 -overwrite -cutline %s -crop_to_cutline %s %s' % (inctyshp, fn_lu, outluras)
    cmd3 = 'gdalwarp -dstnodata 0 -overwrite -cutline %s -crop_to_cutline %s %s' % (inctyshp, fn_sol, outsolras)
    cmd4 = 'gdalwarp -dstnodata 0 -overwrite -cutline %s -crop_to_cutline %s %s' % (inctyshp, fn_zip, outzipras)

    # Build TauDEM commands
    cmd5 = 'mpiexec -n ' + "8" + ' pitremove -z ' + '"' + outdemras + '"' + ' -fel ' + '"' + outfel + '"'
    cmd6 = 'mpiexec -n ' + "8" + ' D8FlowDir -fel ' + '"' + outfel + '"' + ' -p ' + '"' + outp + '"' + ' -sd8 ' + '"' + outsd8 + '"'
	
    # Build raster conversion commands
    cmd7 = 'gdal_translate -of AAIGrid -b 1 %s %s' % (outdemras, outdemasc)
    cmd8 = 'gdal_translate -of AAIGrid -b 1 %s %s' % (outluras, outluasc)
    cmd9 = 'gdal_translate -of AAIGrid -b 1 %s %s' % (outsolras, outsolasc)
    cmd10 = 'gdal_translate -of AAIGrid -b 1 %s %s' % (outsd8, outslpasc)
    cmd11 = 'gdal_translate -of AAIGrid -b 1 %s %s' % (outzipras, outzipasc)

    # Execute commands
    for iter in range(1,12):
        if (devMode == False):
            subprocess.call(eval('cmd' + str(iter)), shell=True)
            print("\n")

# Optionally delete nonessential data output
if delete == str(1):
    if (verb == str(1)):
        print("Deleting nonessential output.")
        print("\n")
    os.remove(outdemras)
    os.remove(outluras)
    os.remove(outsolras)
    os.remove(outsd8)
    os.remove(outfel)
    os.remove(outp)

# Announce program end
if (verb == str(1)):
    print("Finished preparing inputs.")
    print("\n")
