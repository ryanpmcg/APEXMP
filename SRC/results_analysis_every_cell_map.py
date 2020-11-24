# -*- coding: utf-8 -*-
#############################################################################
############################## PROGRAM METADATA #############################
#############################################################################

# Last Updated by: Feng Pan
# Last Updated on: 8 Oct 2020
# Purpose: This script is designed to extract all the results and seperate 
#            them to soil loss, TN, and TP with comparison of different 
#            management plans and orders of differences between each two
#            management plans.
# Contributors: Feng made, debug, finalized the code alone.

#############################################################################
############################# INSTRUCTIONS TO RUN ###########################
#############################################################################

# Requirements: Place results_analysis_(version).py in the working directory. 
#               THE ROOT DIRECTORY PATH MUST NOT CONTAIN ANY SPACES. Requires 
#               gdal and pandas modules (in python 3.x version). In this 
#               directory the following must also be present:
#
# Files or directories (without extention are directories):    
# ----working directory
#               -------RESULTS
#                         --- *.csv (results file from the call_apex_mp code)
#               -------results analysis
#                         ---- * (generated results directory with csv file name)
#                               --- *.tif and *.asc (result maps will be generated)
#
# Output explanation: With this results, all the three types of model simulation
#                       can be compared among different scenarios and the results
#                       can be mapped with the coordinates.


##############################################################################
############################## IMPORT LIBRARIES ##############################
##############################################################################
import glob, os, sys, shutil
import pandas as pd
import datetime
from shutil import copyfile
import numpy as np
import gdal
import gdalconst
from datetime import timedelta
#############################################################################
################################ RUN PROGRAM ################################
#############################################################################

workdir = os.path.dirname(os.path.realpath(sys.argv[0]))
resultslst = glob.glob("%s/*.csv" %(workdir+"\\RESULTS\\everyhru_map")) 

for idx in range(len(resultslst)):
    
    t1 = datetime.datetime.now()
    fn = os.path.split(resultslst[idx])[-1][:-4]
    f = pd.read_csv(resultslst[idx])

    nodata = [-999]
    data = f[f['Precipitation(mm/yr)'].isin(nodata) == False]
    noresults = ["nan"]
    data = data[data['Precipitation(mm/yr)'].isin(noresults) == False]    
    data = data[data['Soil Loss(t/ha)'].isin(noresults) == False]
    data = data[data['Total N(kg/ha)'].isin(noresults) == False]    
    data = data[data['Total P(kg/ha)'].isin(noresults) == False]

    ops = data['Operation'].iloc[-1]
    # runoff=pd.DataFrame()
    soilloss=pd.DataFrame()
    TN=pd.DataFrame()
    TP=pd.DataFrame()
    soillossdata = {}
    TNdata = {}
    TPdata = {}
    soillossreduction = {}
    soillossranking = {}
    TNreduction = {}
    TNranking = {}
    TPreduction = {}
    TPranking = {}

    for opid in range(int(ops)):
        op = int(opid)+1
        oplist = [op]
        subset =  data[data['Operation'].isin(oplist)]
        subsetr = subset.rename(columns={"Runoff(mm/yr)":"Runoff%i(mm/yr)" %(op),
                                        "Soil Loss(t/ha)":"Soil Loss%i(kg/ha)" %(op),
                                        "Total N(kg/ha)":"Total N%i(kg/ha)" %(op), 
                                        "Total P(kg/ha)":"Total P%i(kg/ha)" %(op)})
        if op == 1:
            # runoff = pd.concat([subsetr["RowID_ColumnID"].astype(str),
            #                         subsetr["Runoff%i(mm/yr)" %(op)].astype(float)],
            #                         axis = 1, sort=False)           
            soilloss = pd.concat([subsetr["RowID_ColumnID"].astype(str),
                                    subsetr["Soil Loss%i(kg/ha)" %(op)].astype(float)*1000], 
                                    axis = 1, sort=False)
            TN = pd.concat([subsetr["RowID_ColumnID"].astype(str),
                            subsetr["Total N%i(kg/ha)" %(op)].astype(float)], 
                            axis = 1, sort=False)
            TP = pd.concat([subsetr["RowID_ColumnID"].astype(str), 
                            subsetr["Total P%i(kg/ha)" %(op)].astype(float)], 
                            axis = 1, sort=False)
            soillossdata[opid] = soilloss["Soil Loss%i(kg/ha)" %(op)].astype(int).tolist()
            TNdata[opid] = TN["Total N%i(kg/ha)" %(op)].astype(int).tolist()
            TPdata[opid] = TP["Total P%i(kg/ha)" %(op)].astype(int).tolist()
        else:            
            # runoff["Runoff%i(mm/yr)" %(op)] = subsetr["Runoff%i(mm/yr)" %(op)].values

            soilloss["Soil Loss%i(kg/ha)" %(op)] = subsetr["Soil Loss%i(kg/ha)" %(op)].values
            soilloss["op%i-op1" %(op)] = (soilloss["Soil Loss%i(kg/ha)" %(op)] - soilloss["Soil Loss%i(kg/ha)" %(opid)])
            soilloss["op%i-op1_ranking" %(op)] = soilloss["op%i-op1" %(op)].rank(method = 'first')

            TN["Total N%i(kg/ha)" %(op)] = subsetr["Total N%i(kg/ha)" %(op)].values
            TN["op%i-op1" %(op)] = (TN["Total N%i(kg/ha)" %(op)] - TN["Total N%i(kg/ha)" %(opid)])
            TN["op%i-op1_ranking" %(op)] = TN["op%i-op1" %(op)].rank(method = 'first')

            TP["Total P%i(kg/ha)" %(op)] = subsetr["Total P%i(kg/ha)" %(op)].values
            TP["op%i-op1" %(op)] = (TP["Total P%i(kg/ha)" %(op)] - TP["Total P%i(kg/ha)" %(opid)])
            TP["op%i-op1_ranking" %(op)] = TP["op%i-op1" %(op)].rank(method = 'first')

            soillossdata[opid] = soilloss["Soil Loss%i(kg/ha)" %(op)].astype(int).tolist()
            soillossreduction[opid] = soilloss["op%i-op1" %(op)].astype(int).tolist()
            soillossranking[opid] = soilloss["op%i-op1_ranking" %(op)].astype(int).tolist()
            TNdata[opid] = TN["Total N%i(kg/ha)" %(op)].astype(int).tolist()
            TNreduction[opid] = TN["op%i-op1" %(op)].astype(int).tolist()
            TNranking[opid] = TN["op%i-op1_ranking" %(op)].astype(int).tolist()            
            TPdata[opid] = TP["Total P%i(kg/ha)" %(op)].astype(int).tolist()
            TPreduction[opid] = TP["op%i-op1" %(op)].astype(int).tolist()
            TPranking[opid] = TP["op%i-op1_ranking" %(op)].astype(int).tolist()            

    # get the rowid and colid
    soilloss['Rowid'], soilloss['Colid'] = soilloss['RowID_ColumnID'].str.split('_', 1).str
    rown = soilloss['Rowid'].astype(int).tolist()
    coln = soilloss['Colid'].astype(int).tolist()

    otfd = ""
    if not os.path.exists(workdir+"\\results analysis\\everyhru_map\\"+fn):
        os.makedirs(workdir+"\\results analysis\\everyhru_map\\"+fn)
    otfd = workdir+"\\results analysis\\everyhru_map\\"+fn
    os.chdir(otfd)

    cmd1 = 'gdal_translate -of GTiff ' + '"' + workdir+ '"' + '\\INPUTS\\landuse\\asc\\lu.asc' + ' lu.tif'
    os.system(cmd1)

    for idx in range(len(soillossdata)):

        copyfile("lu.tif", ("soilloss%i.tif" %(idx+1)))
        copyfile("lu.tif", ("TN%i.tif" %(idx+1)))
        copyfile("lu.tif", ("TP%i.tif" %(idx+1)))          

        soillosstif = gdal.Open(("soilloss%i.tif" %(idx+1)), gdalconst.GA_Update)
        TNtif = gdal.Open(("TN%i.tif" %(idx+1)), gdalconst.GA_Update)
        TPtif = gdal.Open(("TP%i.tif" %(idx+1)), gdalconst.GA_Update)
        cols = soillosstif.RasterXSize
        rows = soillosstif.RasterYSize
        soillosstifband = soillosstif.GetRasterBand(1)
        TNtifband = TNtif.GetRasterBand(1)
        TPtifband = TPtif.GetRasterBand(1)
        soillossarray = soillosstifband.ReadAsArray()
        TNarray = TNtifband.ReadAsArray()
        TParray = TPtifband.ReadAsArray()

        # make the whole array equal 0
        soillossarray.fill(0)
        TNarray.fill(0)
        TParray.fill(0)

        # update cell with the existing results              
        soillossarray[rown,coln] = np.array(soillossdata[idx])
        TNarray[rown,coln] = np.array(TNdata[idx])
        TParray[rown,coln] = np.array(TPdata[idx])

        soillosstifband.WriteArray(soillossarray)
        soillosstifband.FlushCache()
        del soillossarray
        soillosstif = None

        TNtifband.WriteArray(TNarray)
        TNtifband.FlushCache()
        del TNarray
        TNtif = None

        TPtifband.WriteArray(TParray)
        TPtifband.FlushCache()
        del TParray
        TPtif = None

        cmd2 = 'gdal_translate -of AAIGrid soilloss%i.tif soilloss%i.asc' %(idx+1,idx+1)
        cmd3 = 'gdal_translate -of AAIGrid TN%i.tif TN%i.asc' %(idx+1,idx+1)
        cmd4 = 'gdal_translate -of AAIGrid TP%i.tif TP%i.asc' %(idx+1,idx+1)
        os.system(cmd2)
        os.system(cmd3)
        os.system(cmd4)

        if idx>0:
            copyfile("lu.tif", ("soillossreduction%i.tif" %(idx)))
            copyfile("lu.tif", ("TNreduction%i.tif" %(idx)))
            copyfile("lu.tif", ("TPreduction%i.tif" %(idx)))
            copyfile("lu.tif", ("soillossranking%i.tif" %(idx)))
            copyfile("lu.tif", ("TNranking%i.tif" %(idx)))
            copyfile("lu.tif", ("TPranking%i.tif" %(idx)))  
            
            soillossreductiontif = gdal.Open(("soillossreduction%i.tif" %(idx)), gdalconst.GA_Update)
            TNreductiontif = gdal.Open(("TNreduction%i.tif" %(idx)), gdalconst.GA_Update)
            TPreductiontif = gdal.Open(("TPreduction%i.tif" %(idx)), gdalconst.GA_Update)
            soillossrankingtif = gdal.Open(("soillossranking%i.tif" %(idx)), gdalconst.GA_Update)
            TNrankingtif = gdal.Open(("TNranking%i.tif" %(idx)), gdalconst.GA_Update)
            TPrankingtif = gdal.Open(("TPranking%i.tif" %(idx)), gdalconst.GA_Update)

            soillossreductiontifband = soillossreductiontif.GetRasterBand(1)
            TNreductiontifband = TNreductiontif.GetRasterBand(1)
            TPreductiontifband = TPreductiontif.GetRasterBand(1)
            soillossreductionarray = soillossreductiontifband.ReadAsArray()
            TNreductionarray = TNreductiontifband.ReadAsArray()
            TPreductionarray = TPreductiontifband.ReadAsArray()
            soillossrankingtifband = soillossrankingtif.GetRasterBand(1)
            TNrankingtifband = TNrankingtif.GetRasterBand(1)
            TPrankingtifband = TPrankingtif.GetRasterBand(1)
            soillossrankingarray = soillossrankingtifband.ReadAsArray()
            TNrankingarray = TNrankingtifband.ReadAsArray()
            TPrankingarray = TPrankingtifband.ReadAsArray()            

            # make the whole array equal 0
            soillossreductionarray.fill(0)
            TNreductionarray.fill(0)
            TPreductionarray.fill(0)
            soillossrankingarray.fill(0)
            TNrankingarray.fill(0)
            TPrankingarray.fill(0)

            # update cell with the existing results              
            soillossreductionarray[rown,coln] = np.array(soillossreduction[idx])
            TNreductionarray[rown,coln] = np.array(TNreduction[idx])
            TPreductionarray[rown,coln] = np.array(TPreduction[idx])

            soillossreductiontifband.WriteArray(soillossreductionarray)
            soillossreductiontifband.FlushCache()
            del soillossreductionarray
            soillossreductiontif = None

            TNreductiontifband.WriteArray(TNreductionarray)
            TNreductiontifband.FlushCache()
            del TNreductionarray
            TNreductiontif = None

            TPreductiontifband.WriteArray(TPreductionarray)
            TPreductiontifband.FlushCache()
            del TPreductionarray
            TPreductiontif = None

            soillossrankingarray[rown,coln] = np.array(soillossranking[idx])
            TNrankingarray[rown,coln] = np.array(TNranking[idx])
            TPrankingarray[rown,coln] = np.array(TPranking[idx])

            soillossrankingtifband.WriteArray(soillossrankingarray)
            soillossrankingtifband.FlushCache()
            del soillossrankingarray
            soillossrankingtif = None

            TNrankingtifband.WriteArray(TNrankingarray)
            TNrankingtifband.FlushCache()
            del TNrankingarray
            TNrankingtif = None

            TPrankingtifband.WriteArray(TPrankingarray)
            TPrankingtifband.FlushCache()
            del TPrankingarray
            TPrankingtif = None            

            cmd5 = 'gdal_translate -of AAIGrid soillossreduction%i.tif soillossreduction%i.asc' %(idx,idx)
            cmd6 = 'gdal_translate -of AAIGrid TNreduction%i.tif TNreduction%i.asc' %(idx,idx)
            cmd7 = 'gdal_translate -of AAIGrid TPreduction%i.tif TPreduction%i.asc' %(idx,idx)
            cmd8 = 'gdal_translate -of AAIGrid soillossranking%i.tif soillossranking%i.asc' %(idx,idx)
            cmd9 = 'gdal_translate -of AAIGrid TNranking%i.tif TNranking%i.asc' %(idx,idx)
            cmd10 = 'gdal_translate -of AAIGrid TPranking%i.tif TPranking%i.asc' %(idx,idx)            
            os.system(cmd5)
            os.system(cmd6)
            os.system(cmd7)
            os.system(cmd8)
            os.system(cmd9)
            os.system(cmd10)            