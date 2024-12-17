@echo off
call ya make -o ..\..\..\..\ && yql-minikql-comp_nodes-ut.exe
if errorlevel 1 goto :fail
echo --- DONE ---
pause
exit /b
:fail
echo --- FAIL ---
pause
