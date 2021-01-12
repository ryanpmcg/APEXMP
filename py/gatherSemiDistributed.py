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
# 					  can be mapped with the x and y coordinates.


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

resultslst = glob.glob("%s/*.csv" %(workdir+"\\RESULTS\\grouphru_map")) 
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
        subsetr = subset.rename(columns={"Soil Loss(t/ha)":"Soil Loss%i(kg/ha)" %(op),
                                        "Total N(kg/ha)":"Total N%i(kg/ha)" %(op), 
                                        "Total P(kg/ha)":"Total P%i(kg/ha)" %(op)})
                                        
        if op == 1:
          
            soilloss = pd.concat([subsetr["Rowid_Colid"].astype(str),
                                    subsetr["Longitude"].astype(float),
                                    subsetr["Latitude"].astype(float),
                                    subsetr["Soil Loss%i(kg/ha)" %(op)].astype(float)], 
                                    axis = 1, sort=False)
            TN = pd.concat([subsetr["Rowid_Colid"].astype(str),
                            subsetr["Longitude"].astype(float),
                            subsetr["Latitude"].astype(float), 
                            subsetr["Total N%i(kg/ha)" %(op)].astype(float)], 
                            axis = 1, sort=False)
            TP = pd.concat([subsetr["Rowid_Colid"].astype(str),
                            subsetr["Longitude"].astype(float),
                            subsetr["Latitude"].astype(float),  
                            subsetr["Total P%i(kg/ha)" %(op)].astype(float)], 
                            axis = 1, sort=False)


            newsoilloss1 = pd.DataFrame(soilloss['Rowid_Colid'].str.split(';').tolist(), index=soilloss["Soil Loss%i(kg/ha)" %(op)]).stack()
            newsoilloss1 = newsoilloss1.reset_index([0, "Soil Loss%i(kg/ha)" %(op)])
            newsoilloss1.columns = ["Soil Loss%i(kg/ha)" %(op), 'Rowid_Colid']
            newsoilloss1 = newsoilloss1[newsoilloss1["Soil Loss%i(kg/ha)" %(op)].isnull() == False]

            newTN1 = pd.DataFrame(TN['Rowid_Colid'].str.split(';').tolist(), index=TN["Total N%i(kg/ha)" %(op)]).stack()
            newTN1 = newTN1.reset_index([0, "Total N%i(kg/ha)" %(op)])
            newTN1.columns = ["Total N%i(kg/ha)" %(op), 'Rowid_Colid']
            newTN1 = newTN1[newTN1["Total N%i(kg/ha)" %(op)].isnull() == False]

            newTP1 = pd.DataFrame(TP['Rowid_Colid'].str.split(';').tolist(), index=TP["Total P%i(kg/ha)" %(op)]).stack()
            newTP1 = newTP1.reset_index([0, "Total P%i(kg/ha)" %(op)])
            newTP1.columns = ["Total P%i(kg/ha)" %(op), 'Rowid_Colid']
            newTP1 = newTP1[newTP1["Total P%i(kg/ha)" %(op)].isnull() == False]  

            Fsoilloss = newsoilloss1
            FTN = newTN1
            FTP = newTP1

        else:                     
            soilloss["Soil Loss%i(kg/ha)" %(op)] = subsetr["Soil Loss%i(kg/ha)" %(op)].values
            newsoilloss2 = pd.DataFrame(soilloss['Rowid_Colid'].str.split(';').tolist(), index=soilloss["Soil Loss%i(kg/ha)" %(op)]).stack()
            newsoilloss2 = newsoilloss2.reset_index([0, "Soil Loss%i(kg/ha)" %(op)])
            newsoilloss2.columns = ["Soil Loss%i(kg/ha)" %(op), 'Rowid_Colid']
            newsoilloss2 = newsoilloss2[newsoilloss2["Soil Loss%i(kg/ha)" %(op)].isnull() == False]           
            newsoilloss2["op%i-op1" %(op)] = (newsoilloss2["Soil Loss%i(kg/ha)" %(op)] - newsoilloss1["Soil Loss%i(kg/ha)" %(opid)])
            newsoilloss2["op%i-op1_ranking" %(op)] = newsoilloss2["op%i-op1" %(op)].rank(method = 'first')

            TN["Total N%i(kg/ha)" %(op)] = subsetr["Total N%i(kg/ha)" %(op)].values
            newTN2 = pd.DataFrame(TN['Rowid_Colid'].str.split(';').tolist(), index=TN["Total N%i(kg/ha)" %(op)]).stack()
            newTN2 = newTN2.reset_index([0, "Total N%i(kg/ha)" %(op)])
            newTN2.columns = ["Total N%i(kg/ha)" %(op), 'Rowid_Colid']
            newTN2 = newTN2[newTN2["Total N%i(kg/ha)" %(op)].isnull() == False]            
            newTN2["op%i-op1" %(op)] = (newTN2["Total N%i(kg/ha)" %(op)] - newTN1["Total N%i(kg/ha)" %(opid)])
            newTN2["op%i-op1_ranking" %(op)] = newTN2["op%i-op1" %(op)].rank(method = 'first')

            TP["Total P%i(kg/ha)" %(op)] = subsetr["Total P%i(kg/ha)" %(op)].values
            newTP2 = pd.DataFrame(TP['Rowid_Colid'].str.split(';').tolist(), index=TP["Total P%i(kg/ha)" %(op)]).stack()
            newTP2 = newTP2.reset_index([0, "Total P%i(kg/ha)" %(op)])
            newTP2.columns = ["Total P%i(kg/ha)" %(op), 'Rowid_Colid']
            newTP2 = newTP2[newTP2["Total P%i(kg/ha)" %(op)].isnull() == False]             
            newTP2["op%i-op1" %(op)] = (newTP2["Total P%i(kg/ha)" %(op)] - newTP1["Total P%i(kg/ha)" %(opid)])
            newTP2["op%i-op1_ranking" %(op)] = newTP2["op%i-op1" %(op)].rank(method = 'first')

            Fsoilloss = pd.concat([Fsoilloss, newsoilloss2.reindex(Fsoilloss.index)], axis=1)
            FTN = pd.concat([FTN, newTN2.reindex(FTN.index)], axis=1)
            FTP = pd.concat([FTP, newTP2.reindex(FTP.index)], axis=1)


    otfd = ""
    if not os.path.exists(workdir+"\\results analysis\\grouphru\\"+fn):
        os.makedirs(workdir+"\\results analysis\\grouphru\\"+fn)
    otfd = workdir+"\\results analysis\\grouphru\\"+fn
    os.chdir(otfd)	
    Fsoilloss.to_csv('soilloss_%s.csv' %(fn))
    FTN.to_csv('TN_%s.csv' %(fn))
    FTP.to_csv('TP_%s.csv' %(fn))