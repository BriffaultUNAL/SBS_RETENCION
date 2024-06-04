#!/bin/bash

LOG_FILE="/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/config_files/app.log"
exec > >(tee -a "$LOG_FILE") 2>&1
start_time=$(date +%s)
echo -e "Inicio de ejecucion: $(date)\n"
#echo "Mounting folder"
#mount -t cifs //172.10.7.5/fs-cos$ /disk/PROCESOS/Compartida/AZTECA -o username=cesar.almeciga,password=Enero2024*,uid=$(id -u),gid=$(id -g)
#echo "Folder mounted succesfully"
cd /home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes
/home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/venv/bin/python3 /home/usr-dwh/Documentos/Procesos/procesos_disk/sbs_retencion_reportes/main.py
echo "Process executed"
#umount -f /disk/PROCESOS/Compartida/AZTECA
#echo -e "Folder unmonted \n"
end_time=$(date +%s)
runtime=$((end_time-start_time))
formatted_runtime=$(date -u -d @"$runtime" +'%M minutos y %S segundos')
echo -e "Fin de ejecucion: $(date)\n"
echo -e "Tiempo de ejecucion: $formatted_runtime \n \n \n"

