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
#               Requires gdal module (python 3.x version preferred).
#               All data must be in the ./APEXMP/INPUTS directory along with
#               the following:
#
# Files or directories (names without extensions are directories):    
# --working directory
#               --INPUTS
#                         --ssurgo2apex.csv      (soil database, provided)
#                         --studyarea 
#                            --studyarea.shp       (studyarea shapefiles, needed)
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
#                         --slope               
#                             --asc               (final maps for each county in study area)
#                             --indi              (intermediate maps, will be deleted at the end)
#
# Data requirement: All of the origianl datasets need to have the same cell
# size, origin, extent, and projection. 


##############################################################################
############################## IMPORT LIBRARIES ##############################
##############################################################################

import os, glob, subprocess, sys


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
fd_studyarea = "studyarea"
fd_dem = "dem"
fd_lu = "landuse"
fd_sol = "soil"
fd_slp = "slope"
fd_data = "original"
indi = "indi"
asc = "asc"

fn_studyarea = os.path.join(fd_gismain, fd_studyarea, "studyarea.shp")
fn_dem = os.path.join(fd_gismain, fd_dem, fd_data, "alldem1.tif")
fn_lu = os.path.join(fd_gismain, fd_lu, fd_data, "alllanduse.tif")
fn_sol = os.path.join(fd_gismain, fd_sol, fd_data, "allsoil.tif")

fd_fdemindi = os.path.join(fd_gismain, fd_dem, indi)
fd_fdemindiasc = os.path.join(fd_gismain, fd_dem, asc)
fd_fluindi = os.path.join(fd_gismain, fd_lu, indi)
fd_fluindiasc = os.path.join(fd_gismain, fd_lu, asc)
fd_fsolindi = os.path.join(fd_gismain, fd_sol, indi)
fd_fsolindiasc = os.path.join(fd_gismain, fd_sol, asc)
fd_fslpindi = os.path.join(fd_gismain, fd_slp, indi)
fd_fslpindiasc = os.path.join(fd_gismain, fd_slp, asc)

outdemras = os.path.join(fd_fdemindi, "dem.tif")
outluras = os.path.join(fd_fluindi, "lu.tif")
outsolras = os.path.join(fd_fsolindi, "sol.tif")
outfel = os.path.join(fd_gismain, fd_slp, indi, "fel.tif")
outp = os.path.join(fd_gismain, fd_slp, indi, "p.tif")
outsd8 = os.path.join(fd_gismain, fd_slp, indi, "slp.tif")

outdemasc = os.path.join(fd_fdemindiasc, "dem.asc")
outluasc = os.path.join(fd_fluindiasc, "lu.asc")
outsolasc = os.path.join(fd_fsolindiasc, "sol.asc")
outslpasc = os.path.join(fd_fslpindiasc, "slp.asc")

# Build raster masking commands
cmd1 = 'gdalwarp -dstnodata 0 -overwrite -cutline %s -crop_to_cutline %s %s' % (fn_studyarea, fn_dem, outdemras)
cmd2 = 'gdalwarp -dstnodata 0 -overwrite -cutline %s -crop_to_cutline %s %s' % (fn_studyarea, fn_lu, outluras)
cmd3 = 'gdalwarp -dstnodata 0 -overwrite -cutline %s -crop_to_cutline %s %s' % (fn_studyarea, fn_sol, outsolras)

# Build TauDEM commands
cmd4 = 'mpiexec -n ' + "8" + ' pitremove -z ' + '"' + outdemras + '"' + ' -fel ' + '"' + outfel + '"'
cmd5 = 'mpiexec -n ' + "8" + ' D8FlowDir -fel ' + '"' + outfel + '"' + ' -p ' + '"' + outp + '"' + ' -sd8 ' + '"' + outsd8 + '"'

# Build raster conversion commands
cmd6 = 'gdal_translate -of AAIGrid -b 1 %s %s' % (outdemras, outdemasc)
cmd7 = 'gdal_translate -of AAIGrid -b 1 %s %s' % (outluras, outluasc)
cmd8 = 'gdal_translate -of AAIGrid -b 1 %s %s' % (outsolras, outsolasc)
cmd9 = 'gdal_translate -of AAIGrid -b 1 %s %s' % (outsd8, outslpasc)

# Execute commands if devMode is off
for iter in range(1,10):
    if (devm == False):
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
