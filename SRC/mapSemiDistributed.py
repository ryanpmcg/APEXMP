# -*- coding: utf-8 -*-
#############################################################################
############################## PROGRAM METADATA #############################
#############################################################################

# Last Updated by: Feng Pan and Ryan McGehee
# Last Updated on: 29 November 2020
# Purpose: This script is designed to extract all the results and seperate 
# 		   them to soil loss, TN, and TP with comparison of different 
# 		   management plans and orders of differences between each two
# 		   management plans.
# Contributors: Feng Pan and Ryan McGehee. This code needs refactoring.


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
import numpy as np
import gdal
import gdalconst
from datetime import timedelta
from shutil import copyfile


#############################################################################
############################## GLOBAL VARIABLES #############################
#############################################################################

workdir = sys.argv[1]
delete = sys.argv[2]
verb = sys.argv[3]
ubmp = sys.argv[4]
bmps = sys.argv[5]
devm = sys.argv[6]
nworkers = int(sys.argv[7])


#############################################################################
################################ RUN PROGRAM ################################
#############################################################################

resultslst = glob.glob("%s/*.csv" %(workdir+"\\RESULTS\\grouphru_map")) 

for idx in range(len(resultslst)):

    # runoff=pd.DataFrame()
    soilloss=pd.DataFrame()
    TN=pd.DataFrame()
    TP=pd.DataFrame()
    TPranking = {}
    stctyn = []
    opsl = []

    t1 = datetime.datetime.now()
    fn = os.path.split(resultslst[idx])[-1][:-4]
    f = pd.read_csv(resultslst[idx])

    otfd = ""
    if not os.path.exists(workdir+"\\results analysis\\grouphru_map\\"+fn):
        os.makedirs(workdir+"\\results analysis\\grouphru_map\\"+fn)
    otfd = workdir+"\\results analysis\\grouphru_map\\"+fn
    os.chdir(otfd)

    nodata = [-999]
    data = f[f['Precipitation(mm/yr)'].isin(nodata) == False]
    noresults = ["nan"]
    data = data[data['Precipitation(mm/yr)'].isin(noresults) == False]    
    data = data[data['Soil Loss(t/ha)'].isin(noresults) == False]
    data = data[data['Total N(kg/ha)'].isin(noresults) == False]    
    data = data[data['Total P(kg/ha)'].isin(noresults) == False]

    ops = data['Operation'].iloc[-1]
    for i in range(int(ops)):
        opsl.append(i)

    soillossdata = dict.fromkeys(opsl)
    TNdata = dict.fromkeys(opsl)
    TPdata = dict.fromkeys(opsl)
    rown = []
    coln = []
    soillossreduction = dict.fromkeys(opsl)
    soillossranking = dict.fromkeys(opsl)
    TNreduction = dict.fromkeys(opsl)
    TNranking = dict.fromkeys(opsl)
    TPreduction = dict.fromkeys(opsl)

    countylist = []
    county = []
    countylist = data["County"].unique().tolist()

    for ctyid in range(len(countylist)):
        stctyn.append(fn.split("_")[0] +"_"+ countylist[ctyid])
        cmd1 = 'gdal_translate -of GTiff ' + '"' + workdir+ '"' + '\\INPUTS\\landuse\\asc\\lu%s.asc' %(stctyn[ctyid]) + ' lu%s.tif' %(stctyn[ctyid])
        os.system(cmd1)

    for opid in range(int(ops)):
        op = int(opid)+1
        oplist = [op]
        subset =  data[data['Operation'].isin(oplist)]
        subsetr = subset.rename(columns={#"Runoff(mm/yr)":"Runoff%i(mm/yr)" %(op),
                                        "Soil Loss(t/ha)":"Soil Loss%i(kg/ha)" %(op),
                                        "Total N(kg/ha)":"Total N%i(kg/ha)" %(op), 
                                        "Total P(kg/ha)":"Total P%i(kg/ha)" %(op)})
        subsetr["Soil Loss%i(kg/ha)" %(op)] = subsetr["Soil Loss%i(kg/ha)" %(op)].astype(float)*1000

        #merge the county and data and then split the data by col and row IDs
        subsetr["county_SL"] = subsetr[["County","Soil Loss%i(kg/ha)" %(op)]].astype(str).agg('_'.join, axis=1)
        subsetr["county_TN"] = subsetr[["County","Total N%i(kg/ha)" %(op)]].astype(str).agg('_'.join, axis=1)
        subsetr["county_TP"] = subsetr[["County","Total P%i(kg/ha)" %(op)]].astype(str).agg('_'.join, axis=1)

        if op == 1:
            # runoff = pd.concat([subsetr["Rowid_Colid"].astype(str),
            #                         subsetr["County"].astype(str),
            #                         subsetr["Runoff%i(mm/yr)" %(op)].astype(float)],
            #                         axis = 1, sort=False)           
            soilloss = pd.concat([subsetr["Rowid_Colid"].astype(str),
                                    subsetr["county_SL"].astype(str)],
                                    # subsetr["Soil Loss%i(kg/ha)" %(op)].astype(float)*1000], 
                                    axis = 1, sort=False)
            TN = pd.concat([subsetr["Rowid_Colid"].astype(str),
                                    subsetr["county_TN"].astype(str)], 
                                    # subsetr["Total N%i(kg/ha)" %(op)].astype(float)], 
                            axis = 1, sort=False)
            TP = pd.concat([subsetr["Rowid_Colid"].astype(str),
                            subsetr["county_TP"].astype(str)], 
                            # subsetr["Total P%i(kg/ha)" %(op)].astype(float)], 
                            axis = 1, sort=False)

            # newrunoff = pd.DataFrame(runoff['Rowid_Colid'].str.split(';').tolist(), index=runoff["Runoff%i(mm/yr)" %(op)]).stack()
            # newrunoff = newrunoff.reset_index([0, "Runoff%i(mm/yr)" %(op)])
            # newrunoff.columns = ["Runoff%i(mm/yr)" %(op), 'Rowid_Colid']
            # newrunoff = newrunoff[newrunoff["Runoff%i(mm/yr)" %(op)].isnull() == False]

            newsoilloss1 = pd.DataFrame(soilloss['Rowid_Colid'].str.split(';').tolist(), index=soilloss["county_SL"]).stack()
            newsoilloss1 = newsoilloss1.reset_index([0, "county_SL"])
            newsoilloss1.columns = ["county_SL", 'Rowid_Colid']
            newsoilloss1 = newsoilloss1[newsoilloss1["county_SL"].isnull() == False]
            newsoilloss1[['County','Soil Loss%i(kg/ha)'%(op)]] = newsoilloss1["county_SL"].str.split('_',expand=True) 

            newTN1 = pd.DataFrame(TN['Rowid_Colid'].str.split(';').tolist(), index=TN["county_TN"]).stack()
            newTN1 = newTN1.reset_index([0, "county_TN"])
            newTN1.columns = ["county_TN", 'Rowid_Colid']
            newTN1 = newTN1[newTN1["county_TN"].isnull() == False]
            newTN1[['County',"Total N%i(kg/ha)" %(op)]] = newTN1["county_TN"].str.split('_',expand=True)            

            newTP1 = pd.DataFrame(TP['Rowid_Colid'].str.split(';').tolist(), index=TP["county_TP"]).stack()
            newTP1 = newTP1.reset_index([0, "county_TP"])
            newTP1.columns = ["county_TP", 'Rowid_Colid']
            newTP1 = newTP1[newTP1["county_TP"].isnull() == False]
            newTP1[['County',"Total P%i(kg/ha)" %(op)]] = newTP1["county_TP"].str.split('_',expand=True)              

            # split data by counties

            for clid in range(len(countylist)):
                
                county = [countylist[clid]]
                SLcounty =  newsoilloss1[newsoilloss1['County'].isin(county)]
                TNcounty =  newTN1[newTN1['County'].isin(county)]
                TPcounty =  newTP1[newTP1['County'].isin(county)]
                soillossdata[op] = []
                TNdata[op] = []
                TPdata[op] = []
                soillossdata[op].append(SLcounty["Soil Loss%i(kg/ha)" %(op)].astype(float).tolist())
                TNdata[op].append(TNcounty["Total N%i(kg/ha)" %(op)].astype(float).tolist())
                TPdata[op].append(TPcounty["Total P%i(kg/ha)" %(op)].astype(float).tolist())
                # get the rowid and colid
                SLcounty['Rowid'], SLcounty['Colid'] = SLcounty['Rowid_Colid'].str.split('_', 1).str
                rown.append(SLcounty['Rowid'].astype(int).tolist())
                coln.append(SLcounty['Colid'].astype(int).tolist())

        else:                     
            soilloss["county_SL"] = subsetr["county_SL"].values
            newsoilloss2 = pd.DataFrame(soilloss['Rowid_Colid'].str.split(';').tolist(), index=soilloss["county_SL"]).stack()
            newsoilloss2 = newsoilloss2.reset_index([0, "county_SL"])
            newsoilloss2.columns = ["county_SL", 'Rowid_Colid']
            newsoilloss2 = newsoilloss2[newsoilloss2["county_SL"].isnull() == False]
            newsoilloss2[['County',"Soil Loss%i(kg/ha)" %(op)]] = newsoilloss2["county_SL"].str.split('_',expand=True)
            newsoilloss2["op%i-op1" %(op)] = (newsoilloss2["Soil Loss%i(kg/ha)" %(op)].astype(float) - newsoilloss1["Soil Loss%i(kg/ha)" %(opid)].astype(float))

            TN["county_TN"] = subsetr["county_TN"].values
            newTN2 = pd.DataFrame(TN['Rowid_Colid'].str.split(';').tolist(), index=TN["county_TN"]).stack()
            newTN2 = newTN2.reset_index([0, "county_TN"])
            newTN2.columns = ["county_TN", 'Rowid_Colid']
            newTN2 = newTN2[newTN2["county_TN"].isnull() == False]
            newTN2[['County',"Total N%i(kg/ha)" %(op)]] = newTN2["county_TN"].str.split('_',expand=True)                         
            newTN2["op%i-op1" %(op)] = (newTN2["Total N%i(kg/ha)" %(op)].astype(float) - newTN1["Total N%i(kg/ha)" %(opid)].astype(float))

            TP["county_TP"] = subsetr["county_TP"].values
            newTP2 = pd.DataFrame(TP['Rowid_Colid'].str.split(';').tolist(), index=TP["county_TP"]).stack()
            newTP2 = newTP2.reset_index([0, "county_TP"])
            newTP2.columns = ["county_TP", 'Rowid_Colid']
            newTP2 = newTP2[newTP2["county_TP"].isnull() == False]
            newTP2[['County',"Total P%i(kg/ha)" %(op)]] = newTP2["county_TP"].str.split('_',expand=True)  
            newTP2["op%i-op1" %(op)] = (newTP2["Total P%i(kg/ha)" %(op)].astype(float) - newTP1["Total P%i(kg/ha)" %(opid)].astype(float))
            
            for clid in range(len(countylist)):
                
                county = [countylist[clid]]
                SLcounty =  newsoilloss2[newsoilloss2['County'].isin(county)]
                TNcounty =  newTN2[newTN2['County'].isin(county)]
                TPcounty =  newTP2[newTP2['County'].isin(county)]
                soillossdata[op] = []
                TNdata[op] = []
                TPdata[op] = []
                soillossreduction[op] = []
                TNreduction[op] = []
                TPreduction[op] = []
                soillossranking[op] = []
                TNranking[op] = []
                TPranking[op] = []

                SLcounty["op%i-op1_ranking" %(op)] = SLcounty["op%i-op1" %(op)].rank(method = 'first')
                soillossdata[op].append(SLcounty["Soil Loss%i(kg/ha)" %(op)].astype(float).tolist())
                soillossreduction[op].append(SLcounty["op%i-op1" %(op)].astype(float).tolist())
                soillossranking[op].append(SLcounty["op%i-op1_ranking" %(op)].astype(int).tolist())   

                TNcounty["op%i-op1_ranking" %(op)] = TNcounty["op%i-op1" %(op)].rank(method = 'first')
                TNdata[op].append(TNcounty["Total N%i(kg/ha)" %(op)].astype(float).tolist())
                TNreduction[op].append(TNcounty["op%i-op1" %(op)].astype(float).tolist())
                TNranking[op].append(TNcounty["op%i-op1_ranking" %(op)].astype(int).tolist()) 

                TPcounty["op%i-op1_ranking" %(op)] = TPcounty["op%i-op1" %(op)].rank(method = 'first')  
                TPdata[op].append(TPcounty["Total P%i(kg/ha)" %(op)].astype(float).tolist())
                TPreduction[op].append(TPcounty["op%i-op1" %(op)].astype(float).tolist())
                TPranking[op].append(TPcounty["op%i-op1_ranking" %(op)].astype(int).tolist())  

                # # get the rowid and colid
                # SLcounty['Rowid'], SLcounty['Colid'] = SLcounty['Rowid_Colid'].str.split('_', 1).str
                # rown.append(SLcounty['Rowid'].astype(int).tolist())
                # coln.append(SLcounty['Colid'].astype(int).tolist())  

    for opid in range(len(opsl)):
        op = int(opid)+1
        for idx in range(len(stctyn)):

            copyfile("lu%s.tif"%(stctyn[idx]), ("soilloss%s.tif" %(stctyn[idx]+str(op))))
            copyfile("lu%s.tif"%(stctyn[idx]), ("TN%s.tif" %(stctyn[idx]+str(op))))
            copyfile("lu%s.tif"%(stctyn[idx]), ("TP%s.tif" %(stctyn[idx]+str(op))))          

            soillosstif = gdal.Open(("soilloss%s.tif" %(stctyn[idx]+str(op))), gdalconst.GA_Update)
            TNtif = gdal.Open(("TN%s.tif" %(stctyn[idx]+str(op))), gdalconst.GA_Update)
            TPtif = gdal.Open(("TP%s.tif" %(stctyn[idx]+str(op))), gdalconst.GA_Update)
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
            soillossarray[rown[idx],coln[idx]] = np.array(soillossdata[op][idx])
            TNarray[rown[idx],coln[idx]] = np.array(TNdata[op][idx])
            TParray[rown[idx],coln[idx]] = np.array(TPdata[op][idx])

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

            cmd2 = 'gdal_translate -of AAIGrid soilloss%s.tif soilloss%s.asc' %(stctyn[idx]+str(op),stctyn[idx]+str(op))
            cmd3 = 'gdal_translate -of AAIGrid TN%s.tif TN%s.asc' %(stctyn[idx]+str(op),stctyn[idx]+str(op))
            cmd4 = 'gdal_translate -of AAIGrid TP%s.tif TP%s.asc' %(stctyn[idx]+str(op),stctyn[idx]+str(op))
            os.system(cmd2)
            os.system(cmd3)
            os.system(cmd4)

            if op>1:
                copyfile(("lu%s.tif" %(stctyn[idx])), ("soillossreduction%s.tif" %(stctyn[idx]+str(op))))
                copyfile(("lu%s.tif" %(stctyn[idx])), ("TNreduction%s.tif" %(stctyn[idx]+str(op))))
                copyfile(("lu%s.tif" %(stctyn[idx])), ("TPreduction%s.tif" %(stctyn[idx]+str(op))))
                copyfile(("lu%s.tif" %(stctyn[idx])), ("soillossranking%s.tif" %(stctyn[idx]+str(op))))
                copyfile(("lu%s.tif" %(stctyn[idx])), ("TNranking%s.tif" %(stctyn[idx]+str(op))))
                copyfile(("lu%s.tif" %(stctyn[idx])), ("TPranking%s.tif" %(stctyn[idx]+str(op))))  
                
                soillossreductiontif = gdal.Open(("soillossreduction%s.tif" %(stctyn[idx]+str(op))), gdalconst.GA_Update)
                TNreductiontif = gdal.Open(("TNreduction%s.tif" %(stctyn[idx]+str(op))), gdalconst.GA_Update)
                TPreductiontif = gdal.Open(("TPreduction%s.tif" %(stctyn[idx]+str(op))), gdalconst.GA_Update)
                soillossrankingtif = gdal.Open(("soillossranking%s.tif" %(stctyn[idx]+str(op))), gdalconst.GA_Update)
                TNrankingtif = gdal.Open(("TNranking%s.tif" %(stctyn[idx]+str(op))), gdalconst.GA_Update)
                TPrankingtif = gdal.Open(("TPranking%s.tif" %(stctyn[idx]+str(op))), gdalconst.GA_Update)

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
                soillossreductionarray[rown[idx],coln[idx]] = np.array(soillossreduction[op][idx])
                TNreductionarray[rown[idx],coln[idx]] = np.array(TNreduction[op][idx])
                TPreductionarray[rown[idx],coln[idx]] = np.array(TPreduction[op][idx])

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

                soillossrankingarray[rown[idx],coln[idx]] = np.array(soillossranking[op][idx])
                TNrankingarray[rown[idx],coln[idx]] = np.array(TNranking[op][idx])
                TPrankingarray[rown[idx],coln[idx]] = np.array(TPranking[op][idx])

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

                cmd5 = 'gdal_translate -of AAIGrid soillossreduction%s.tif soillossreduction%s.asc' %(stctyn[idx]+str(op),stctyn[idx]+str(op))
                cmd6 = 'gdal_translate -of AAIGrid TNreduction%s.tif TNreduction%s.asc' %(stctyn[idx]+str(op),stctyn[idx]+str(op))
                cmd7 = 'gdal_translate -of AAIGrid TPreduction%s.tif TPreduction%s.asc' %(stctyn[idx]+str(op),stctyn[idx]+str(op))
                cmd8 = 'gdal_translate -of AAIGrid soillossranking%s.tif soillossranking%s.asc' %(stctyn[idx]+str(op),stctyn[idx]+str(op))
                cmd9 = 'gdal_translate -of AAIGrid TNranking%s.tif TNranking%s.asc' %(stctyn[idx]+str(op),stctyn[idx]+str(op))
                cmd10 = 'gdal_translate -of AAIGrid TPranking%s.tif TPranking%s.asc' %(stctyn[idx]+str(op),stctyn[idx]+str(op))            
                os.system(cmd5)
                os.system(cmd6)
                os.system(cmd7)
                os.system(cmd8)
                os.system(cmd9)
                os.system(cmd10)            