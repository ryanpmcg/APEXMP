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
#               pandas module (in python 3.x version). In this directory 
# 				the following must also be present:
#
# Files or directories (without extention are directories):    
# ----working directory
#               -------RESULTS
#                         --- *.csv (results file from the call_apex_mp code)
#               -------results analysis
#               		  ---- * (generated results directory with csv file name)
#                               --- *.csv (results analysis files will be generated)
#
# Output explanation: With this results, all the three types of model simulation
# 					  can be compared among different scenarios and the results
# 					  can be mapped with the coordinates.


##############################################################################
############################## IMPORT LIBRARIES ##############################
##############################################################################

import glob, os, sys
import pandas as pd


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

resultslst = glob.glob("%s/*.csv" %(workdir+"\\RESULTS\\everyhru_map")) 
resultsname = []  
for idx in range(len(resultslst)):
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
    runoff=pd.DataFrame()
    soilloss=pd.DataFrame()
    TN=pd.DataFrame()
    TP=pd.DataFrame()

    for opid in range(int(ops)):
        op = int(opid)+1
        oplist = [op]
        subset =  data[data['Operation'].isin(oplist)]
        subsetr = subset.rename(columns={"Runoff(mm/yr)":"Runoff%i(mm/yr)" %(op),
                                        "Soil Loss(t/ha)":"Soil Loss%i(t/ha)" %(op),
                                        "Total N(kg/ha)":"Total N%i(kg/ha)" %(op), 
                                        "Total P(kg/ha)":"Total P%i(kg/ha)" %(op)})
        if op == 1:
            runoff = pd.concat([subsetr["RunID"].astype(str),
                                subsetr["RowID_ColumnID"].astype(str),
                                subsetr["Longitude"].astype(float),
                                subsetr["Latitude"].astype(float),
                                subsetr["Runoff%i(mm/yr)" %(op)].astype(float)],
                                axis = 1, sort=False)           
            soilloss = pd.concat([subsetr["RunID"].astype(str),
                                  subsetr["RowID_ColumnID"].astype(str),
                                  subsetr["Longitude"].astype(float),
                                  subsetr["Latitude"].astype(float),
                                  subsetr["Soil Loss%i(t/ha)" %(op)].astype(float)], 
                                  axis = 1, sort=False)
            TN = pd.concat([subsetr["RunID"].astype(str),
                            subsetr["RowID_ColumnID"].astype(str),
                            subsetr["Longitude"].astype(float),
                            subsetr["Latitude"].astype(float), 
                            subsetr["Total N%i(kg/ha)" %(op)].astype(float)], 
                            axis = 1, sort=False)
            TP = pd.concat([subsetr["RunID"].astype(str),
                            subsetr["RowID_ColumnID"].astype(str),
                            subsetr["Longitude"].astype(float),
                            subsetr["Latitude"].astype(float),                             
                            subsetr["Total P%i(kg/ha)" %(op)].astype(float)], 
                            axis = 1, sort=False)
        else:            
            runoff["Runoff%i(mm/yr)" %(op)] = subsetr["Runoff%i(mm/yr)" %(op)].values

            soilloss["Soil Loss%i(t/ha)" %(op)] = subsetr["Soil Loss%i(t/ha)" %(op)].values
            soilloss["op%i-op1" %(op)] = (soilloss["Soil Loss%i(t/ha)" %(op)] - soilloss["Soil Loss%i(t/ha)" %(opid)])
            soilloss["op%i-op1_ranking" %(op)] = soilloss["op%i-op1" %(op)].rank(method = 'first')

            TN["Total N%i(kg/ha)" %(op)] = subsetr["Total N%i(kg/ha)" %(op)].values
            TN["op%i-op1" %(op)] = (TN["Total N%i(kg/ha)" %(op)] - TN["Total N%i(kg/ha)" %(opid)])
            TN["op%i-op1_ranking" %(op)] = TN["op%i-op1" %(op)].rank(method = 'first')

            TP["Total P%i(kg/ha)" %(op)] = subsetr["Total P%i(kg/ha)" %(op)].values
            TP["op%i-op1" %(op)] = (TP["Total P%i(kg/ha)" %(op)] - TP["Total P%i(kg/ha)" %(opid)])
            TP["op%i-op1_ranking" %(op)] = TP["op%i-op1" %(op)].rank(method = 'first')

    otfd = ""
    if not os.path.exists(workdir+"\\results analysis\\everyhru\\"+fn):
        os.makedirs(workdir+"\\results analysis\\everyhru\\"+fn)
    otfd = workdir+"\\results analysis\\everyhru\\"+fn
    os.chdir(otfd)
	
    soilloss.to_csv('soilloss_%s.csv' %(fn))
    TN.to_csv('TN_%s.csv' %(fn))
    TP.to_csv('TP_%s.csv' %(fn))
    runoff.to_csv('runoff_%s.csv' %(fn))