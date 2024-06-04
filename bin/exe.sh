#!/bin/bash

ACT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd)"

echo $ACT_DIR

LOG_FILE="$ACT_DIR/config_files/logs_main.log"
exec > >(tee -a "$LOG_FILE") 2>&1

start_time=$(date +%s)

echo -e "Inicio de ejecucion: $(date)\n"

#rm -rf /home/usr-dwh/Escritorio/Compartida_L/OPERACIONES/SBS\ INBOUND/ASESORES/Bases/BASES\ SPONSOR/BASES_VANTI/*
rm -rf $ACT_DIR/data/*/*
#mkdir -p /home/usr-dwh/Escritorio/Compartida_L/OPERACIONES/SBS\ INBOUND/ASESORES/Bases/BASES\ SPONSOR/BASES_VANTI/{eca,edeq,emcali,epsa,pereria,vanti}
cd $ACT_DIR

if ! mountpoint -q "$ACT_DIR/shared";
then
    
    echo "Mounting folder"
    #sudo mount -t cifs //172.10.7.5/FS_COS2 "$ACT_DIR/shared" -o username=cesar.almeciga,password=Marzo2024*,uid=$(id -u),gid=$(id -g)

else
    echo "Folder mounted succesfully"
fi



VENV="$ACT_DIR/venv"
source "$VENV/bin/activate"
python3 "$ACT_DIR/main.py"
deactivate

echo "Process executed"

#sudo umount -f "$ACT_DIR/shared"
echo -e "Folder unmonted \n"

end_time=$(date +%s)
runtime=$((end_time-start_time))
formatted_runtime=$(date -u -d @"$runtime" +'%M minutos y %S segundos')
echo -e "Fin de ejecucion: $(date)\n"
echo -e "Tiempo de ejecucion: $formatted_runtime \n \n \n"