# APEXMP
The Agricultural Policy Extender Model Multiprocessing Program (APEXMP) is a Python-based command-line tool designed to execute large numbers of APEX model runs in parallel.

## HOW TO INSTALL
Step 1: Download and move the whole APEXMP folder to "C:/APEXMP".
Step 2: Install "C:/APEXMP/INSTALL/TauDEM537_setup.exe" with "typical" option.
Step 3: For the tutorial runs, unzip the INPUT.7z file as the "C:/APEXMP/INPUTS" folder; For the other runs, download the whole database from https://app.globus.org/file-manager?origin_id=001e9a6c-512e-11eb-87b7-02187389bd35&origin_path=%2F, make it as "C:/APEXMP/INPUTS", and unzip all the zip files in the "./original" folders.

## HOW TO RUN
Simply place your "StudyArea.shp" shapefile in the "C:/APEXMP/INPUTS/StudyArea" folder, then double-click the "RunAPEXMP.bat" batch file, and a command prompt window should appear while the job is being executed.

## HOW TO CUSTOMIZE A RUN
Change "C:/APEXMP/CONFIGS/DefaultConfig.txt" to your preferred options, then double-click the "RunAPEXMP.bat" batch file, and a command prompt window should appear while the job is being executed.

## TUTORIAL
A tutorial batch file was provided to demonstrate the APEXMP program execution in four different scenarios which include both large and small jobs in distributed and semi-distributed configurations, respectively. To run the tutorial, simply double-click the "AutoTutorial.bat" batch file, and a command prompt window should appear while the jobs are being executed.
## Documentation
The APEXMP documentation with specific details is provided as "C:/APEXMP/APEXMP Documentation.pdf".
