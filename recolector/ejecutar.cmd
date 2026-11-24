@echo off
rem ------------------------------------------------------------------
rem Wrapper para el Programador de Tareas de Windows (equivale al crontab).
rem Uso:  ejecutar.cmd <fuente>     (estado, tramos, espiras, aire, meteo, aemet)
rem Se situa en la raiz del proyecto y redirige la salida a logs\cron_<fuente>.log
rem ------------------------------------------------------------------
cd /d "%~dp0.."
if not exist logs mkdir logs
.venv\Scripts\python.exe recolector\recolector.py %1 >> logs\cron_%1.log 2>&1
