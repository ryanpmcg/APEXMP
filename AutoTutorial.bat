@ECHO OFF
CD "C:\APEXMP"

COPY "C:\APEXMP\CONFIGS\SemiDistSmallJob.txt" "C:\APEXMP\config.txt"
APEXMP.exe

COPY "C:\APEXMP\CONFIGS\DistSmallJob.txt" "C:\APEXMP\config.txt"
APEXMP.exe

COPY "C:\APEXMP\CONFIGS\SemiDistLargeJob.txt" "C:\APEXMP\config.txt"
APEXMP.exe

COPY "C:\APEXMP\CONFIGS\DistLargeJob.txt" "C:\APEXMP\config.txt"
APEXMP.exe

COPY "C:\APEXMP\CONFIGS\DefaultConfig.txt" "C:\APEXMP\config.txt"
