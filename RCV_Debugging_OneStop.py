# Author: Victor <Victor.Garcia@solidigm.com>

import os
import re
import csv
import json
import shutil
import zipfile
import logging
import getpass
import urllib3
import paramiko
import subprocess
import time
from stat import S_ISDIR
from tqdm import tqdm
from datetime import datetime, timedelta
from jira import JIRA

class TerminalCommands:
    """Class encapsulating terminal commands."""
    commands = [
        ["py", "-m", "pip", "install", "-U", "pip"],
        ["py", "-m", "pip", "install", "jira", "urllib3", "paramiko", "logging",
         "shutil", "zipfile", "re", "os", "subprocess", "datetime", "tqdm"]
    ]

def run_terminal_commands(commands):
    """Function to run a list of terminal commands."""
    for command in commands:
        try:
            print(f"Running command: {' '.join(command)}")
            result = subprocess.run(
                command,
                check=True,
                text=True,
                capture_output=True
            )
            print("Output:", result.stdout.strip())
            if result.stderr.strip():
                print("Error Output (if any):", result.stderr.strip())
        except subprocess.CalledProcessError as e:
            print("An error occurred while running the command:")
            print("Command:", ' '.join(command))
            print("Return code:", e.returncode)
            print("Output:", e.output.strip())
            if e.stderr.strip():
                print("Error:", e.stderr.strip())
        print("=" * 50)

run_terminal_commands(TerminalCommands.commands)

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Deshabilitar advertencias de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# -----------------------------------------
# Funciones existentes del script original
# -----------------------------------------

def fetch_credentials():
    """Obtiene credenciales del usuario de forma segura."""
    usuario = input("Ingrese su usuario Jira: ")
    contrasena = getpass.getpass("Ingrese su contraseña Jira: ")
    return usuario, contrasena


def initialize_jira_connection(usuario, contrasena):
    """Conecta al servidor Jira."""
    options = {"server": "https://npsg-jira.elements.local", "verify": False}
    try:
        jira = JIRA(options, basic_auth=(usuario, contrasena))
        logging.info("Conexión establecida con Jira.")
        return jira
    except Exception as e:
        logging.error("No se pudo conectar a Jira: %s", e)
        raise


def extract_paths_and_ip_desde_archivo(file_path):
    """
    Extrae job_id, subfolder_id y dirección IP desde un archivo de descripción local.
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        limpio = re.sub(r"\{[^}]+\}", "", content)
        limpio = limpio.replace("*", "").strip()

        job_pattern = re.compile(r"http://stax-mzm-utf\.elements\.local/#/jobs/(\d+)#task_tests_(\d+)")
        match = job_pattern.search(limpio)
        job_id, subfolder_id = (match.groups() if match else (None, None))

        ip_match = re.search(r"IP:\s*(\d{1,3}(?:\.\d{1,3}){3})", limpio)
        ip_address = ip_match.group(1) if ip_match else None

        return job_id, subfolder_id, ip_address

    except Exception as e:
        logging.error(f"Error al extraer IP desde archivo local: {e}")
        return None, None, None


def extraer_ip_desde_descripcion(file_path):
    """
    Extrae la IP desde jira-description.txt ignorando etiquetas y formato complejo.
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        # Elimina todas las etiquetas { ... }
        limpio = re.sub(r"\{[^}]+\}", "", content)

        # Elimina asteriscos y espacios extra
        limpio = limpio.replace("*", "").strip()

        # Busca la línea que contiene 'IP:' y extrae la IP
        ip_match = re.search(r"IP:\s*([\d]+\.[\d]+\.[\d]+\.[\d]+)", limpio)
        if ip_match:
            ip = ip_match.group(1)
            logging.info(f"IP extraída correctamente: {ip}")
            return ip
        else:
            logging.warning("No se encontró una IP en el archivo de descripción.")
            return None
    except Exception as e:
        logging.error(f"Error al extraer IP desde jira-description.txt: {e}")
        return None


def extract_paths_and_ip(jira, ticket_id):
    import re
    import logging

    try:
        issue = jira.issue(ticket_id)
        descripcion = issue.fields.description
        logging.info("Descripción del ticket obtenida.")

        # Buscar URL de STAX
        match = re.search(r"https?://[^ ]+/#/jobs/(\d+)(#task_tests_(\d+))?", descripcion)
        if not match:
            logging.error("No se encontró una URL válida de STAX en la descripción.")
            return None, None, None

        job_id = match.group(1)
        subfolder_id = match.group(3)

        if not subfolder_id:
            logging.warning("No se especificó subfolder_id en la descripción. Asignando '0' por defecto.")
            subfolder_id = "0"

        # Buscar IP (opcional, si está en la descripción)
        ip_match = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", descripcion)
        ip_address = ip_match.group(0) if ip_match else None

        return job_id, subfolder_id, ip_address

    except Exception as e:
        logging.error(f"Error al extraer información del ticket: {e}")
        return None, None, None


def copy_network_folder(job_id, subfolder_id, folder_name):
    network_path = f"\\\\elements.local\\PV\\RCV_Logs\\rcv_dat_logs\\automated\\STAX_Guadalajara_UTF\\{job_id}\\{subfolder_id}"
    local_path = os.path.join(os.getcwd(), folder_name)

    try:
        if not os.path.exists(network_path):
            logging.error(f"La ruta de red no existe: {network_path}")
            return None

        os.makedirs(local_path, exist_ok=True)

        # Recorrer todos los archivos y subarchivos
        archivos_a_copiar = []
        for root, _, files in os.walk(network_path):
            for file in files:
                ruta_origen = os.path.join(root, file)
                ruta_relativa = os.path.relpath(ruta_origen, network_path)
                ruta_destino = os.path.join(local_path, ruta_relativa)
                archivos_a_copiar.append((ruta_origen, ruta_destino))

        if not archivos_a_copiar:
            logging.warning(f"La carpeta de red está vacía: {network_path}")
            return local_path

        logging.info(f"Iniciando copia de {len(archivos_a_copiar)} archivos (incluyendo subcarpetas)...")

        for origen, destino in tqdm(archivos_a_copiar, desc="Copiando archivos", unit="archivo"):
            os.makedirs(os.path.dirname(destino), exist_ok=True)
            shutil.copy2(origen, destino)

        logging.info(f"Copia completada en: {local_path}")
        return local_path

    except Exception as e:
        logging.error(f"Error al copiar archivos: {e}")
        return None
        
        
def download_messages_via_ssh(ip_address, folder_name):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip_address, username='root', password='sn1ckers?')

        sftp = ssh.open_sftp()
        remote_dir = '/var/log'
        os.makedirs(folder_name, exist_ok=True)
        
        #Listar archivos en /var/log que comiencen con 'messages'
        for filename in sftp.listdir(remote_dir):
            print("Archivo encontrado:", filename) 
            if filename.startswith('messages'):
                remote_file_path = f"{remote_dir}/{filename}"
                local_file_path = os.path.join(folder_name, filename)
                sftp.get(remote_file_path, local_file_path)
                logging.info(f"Archivo '{filename}' descargado desde {ip_address} a {local_file_path}")
        
        sftp.close()
        ssh.close()
    except Exception as e:
        logging.error("Error al descargar el archivo por SSH: %s", e)
 
 
def establecer_conexion_ssh(ip, username='root', password='sn1ckers?'):
    import paramiko
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=username, password=password)
    sftp = ssh.open_sftp()
    return ssh, sftp


def generar_messages_filtrados_remoto(ssh, sftp, ip, start_time, end_time, folder_name):

    try:
        logging.info("Ejecutando comando remoto para ordenar y filtrar mensajes...")

        # Formato para awk
        fecha_obj = time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        mes = time.strftime("%b", fecha_obj)
        dia = time.strftime("%d", fecha_obj)
        hora_inicio = time.strftime("%H:%M:%S", fecha_obj)

        fecha_obj_fin = time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        hora_fin = time.strftime("%H:%M:%S", fecha_obj_fin)

        logging.info(f"Filtro: {mes} {dia} desde {hora_inicio} hasta {hora_fin}")

        # Paso 1: concatenar y ordenar
        comando_ordenar = "cat /var/log/messages* | sort -k1,1M -k2,2n -k3,3n > /tmp/allmessagesOrdenados.txt"
        stdin, stdout, stderr = ssh.exec_command(comando_ordenar)
        stdout.channel.recv_exit_status()

        # Paso 2: verificar si el archivo tiene contenido
        comando_wc = "wc -l /tmp/allmessagesOrdenados.txt"
        stdin, stdout, stderr = ssh.exec_command(comando_wc)
        stdout.channel.recv_exit_status()
        line_count_output = stdout.read().decode().strip()
        match = re.search(r"^\s*(\d+)", line_count_output)
        line_count = int(match.group(1)) if match else 0

        logging.info(f"Líneas en allmessagesOrdenados.txt: {line_count}")
        if line_count == 0:
            logging.warning("El archivo de mensajes está vacío según wc -l.")
            return

        # Paso 3: normalizar días del 1 al 9 usando awk
        comando_normalizar = (
            "awk '{if (match($2, /^[0-9]$/)) $2 = \"0\" $2; print}' /tmp/allmessagesOrdenados.txt > /tmp/messages_normalizados.txt"
        )
        stdin, stdout, stderr = ssh.exec_command(comando_normalizar)
        stdout.channel.recv_exit_status()

        # Paso 4: filtrar por fecha y hora
        comando_filtrar = (
            f"""awk '$1 == "{mes}" && $2 == "{dia}" && $3 >= "{hora_inicio}" && $3 <= "{hora_fin}"' /tmp/messages_normalizados.txt > /tmp/messages_filtrados.txt"""
        )
        stdin, stdout, stderr = ssh.exec_command(comando_filtrar)
        stdout.channel.recv_exit_status()

        # Paso 5: verificar si el archivo filtrado tiene contenido
        comando_wc_filtrado = "wc -l /tmp/messages_filtrados.txt"
        stdin, stdout, stderr = ssh.exec_command(comando_wc_filtrado)
        stdout.channel.recv_exit_status()
        filtrado_count_output = stdout.read().decode().strip()
        match_filtrado = re.search(r"^\s*(\d+)", filtrado_count_output)
        filtrado_count = int(match_filtrado.group(1)) if match_filtrado else 0

        logging.info(f"Líneas filtradas: {filtrado_count}")
        if filtrado_count == 0:
            logging.warning("El archivo filtrado no contiene líneas dentro del rango especificado.")

        # Paso 6: descargar archivos
        logging.info("Descargando archivos filtrados y ordenados...")
        sftp.get("/tmp/allmessagesOrdenados.txt", os.path.join(folder_name, "allmessagesOrdenados.txt"))
        sftp.get("/tmp/messages_filtrados.txt", os.path.join(folder_name, "messages_filtrados.txt"))
        logging.info(f"Archivos descargados en: {folder_name}")

        # Paso 7: limpiar temporales
        ssh.exec_command("rm /tmp/allmessagesOrdenados.txt /tmp/messages_normalizados.txt /tmp/messages_filtrados.txt")
        logging.info("Archivos temporales eliminados del servidor.")

    except Exception as e:
        logging.error(f"Error al generar y descargar messages filtrados: {e}")


def descomprimir_zip_en_carpeta(carpeta):
    import zipfile
    import os

    for root, _, files in os.walk(carpeta):
        for file in files:
            if file.endswith(".zip"):
                zip_path = os.path.join(root, file)
                extract_path = os.path.join(root, file.replace(".zip", ""))
                os.makedirs(extract_path, exist_ok=True)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)
    return carpeta  # o extract_path si quieres trabajar solo con descomprimidos
     

def consolidar_y_filtrar(carpeta, patron_regex):
    try:
        patron = re.compile(patron_regex, re.IGNORECASE)
        archivo_consolidado = os.path.join(carpeta, "Drive_Access_tracker_consolidado.txt")
        resumen_lineas = []

        if not os.path.exists(carpeta):
            logging.error("La carpeta especificada no existe: %s", carpeta)
            return

        with open(archivo_consolidado, "w", encoding="utf-8") as salida:
            for root, _, archivos in os.walk(carpeta):
                for nombre in sorted(archivos):
                    if patron.match(nombre):
                        ruta_archivo = os.path.join(root, nombre)

                        try:
                            # Abrir cada archivo con manejo robusto
                            with open(ruta_archivo, "r", encoding="utf-8", errors="ignore") as entrada:
                                lineas = entrada.readlines()
                                salida.write(f"--- Contenido de: {nombre} ---\n")
                                salida.writelines(lineas)
                                salida.write("\n")
                                resumen_lineas.append((nombre, len(lineas)))

                        except Exception as e:
                            logging.error(f"Error al leer el archivo {ruta_archivo}: {e}")

            salida.write("\n===== RESUMEN DE LÍNEAS POR ARCHIVO =====\n")
            for nombre, cantidad in resumen_lineas:
                salida.write(f"{nombre}: {cantidad} líneas\n")

        logging.info("Archivo consolidado generado: %s", archivo_consolidado)

        # Procesar el archivo consolidado
        sin_J = remove_lines_with_J(archivo_consolidado)
        logging.info("Archivo sin líneas con 'J': %s", sin_J)

        columnas = split_into_columns(sin_J)
        logging.info("Archivo con columnas separadas: %s", columnas)

        filtrado = analyze_and_filter_lines(columnas, archivo_consolidado)
        logging.info("Archivo filtrado generado: %s", filtrado)

    except Exception as e:
        logging.error("Error general durante el procesamiento: %s", e)
        raise


def remove_lines_with_J(file_path):
    try:
        # Abrir el archivo con una codificación más robusta y manejar errores.
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()

        directory, original_filename = os.path.split(file_path)
        name, extension = os.path.splitext(original_filename)
        modified_content = os.path.join(directory, f"{name}_no_J{extension}")

        # Guardar el archivo sin las líneas que contienen "J"
        with open(modified_content, 'w', encoding='utf-8') as file:
            for line in lines:
                if 'J' not in line:
                    file.write(line)

        return modified_content

    except Exception as e:
        logging.error(f"Error en `remove_lines_with_J` al procesar {file_path}: {e}")
        raise


def split_into_columns(content_file):
    try:
        # Abrir el archivo con codificación robusta
        with open(content_file, 'r', encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()

        columns = [line.split() for line in lines]
        directory, original_filename = os.path.split(content_file)
        name, extension = os.path.splitext(original_filename)
        output_file = os.path.join(directory, f"{name}_columns{extension}")

        # Escribir las columnas separadas
        with open(output_file, 'w', encoding='utf-8') as file:
            for column in columns:
                file.write('\t'.join(column) + '\n')

        return output_file

    except Exception as e:
        logging.error(f"Error en `split_into_columns` al procesar {content_file}: {e}")
        raise


def analyze_and_filter_lines(content_file, original_file_path):
    with open(content_file, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    column_dict = {}
    for line in lines:
        columns = line.split()
        if len(columns) >= 5:
            column_value = columns[4]
            if column_value in column_dict:
                column_dict[column_value].append(line)
            else:
                column_dict[column_value] = [line]
    directory, original_filename = os.path.split(original_file_path)
    name, extension = os.path.splitext(original_filename)
    new_filename = f"{name}_parsed{extension}"
    new_file_path = os.path.join(directory, new_filename)
    with open(new_file_path, 'w', encoding='utf-8') as file:
        for key, value in column_dict.items():
            if len(value) > 1 and 'SC=0x00' in value[1] and 'SCT=0x0' in value[1]:
                file.write(value[0])
                file.write(value[1])
    return new_file_path


def extraer_timestamps_y_generar_comando(ip_address, job_id, subfolder_id, folder_name):
    """
    Busca UTFManager.log en la carpeta local, extrae el primer y último timestamp
    para definir el intervalo de tiempo. Devuelve ambos valores en formato YYYY-MM-DD HH:MM:SS.
    """
    try:
        utf_path = None
        for root, _, files in os.walk(folder_name):
            if "UTFManager.log" in files:
                utf_path = os.path.join(root, "UTFManager.log")
                break

        if not utf_path:
            logging.warning("No se encontró UTFManager.log en la carpeta.")
            return None, None

        with open(utf_path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        timestamp_pattern = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
        timestamps = [timestamp_pattern.search(line).group(0) for line in lines if timestamp_pattern.search(line)]

        if not timestamps:
            logging.warning("No se encontraron timestamps en UTFManager.log.")
            return None, None

        start_time = timestamps[0]
        end_time = timestamps[-1]

        logging.info(f"Intervalo de tiempo extraído: {start_time} -> {end_time}")
        print(f"Intervalo detectado: {start_time} -> {end_time}")

        # Generar comando journalctl (solo para mostrar)
        job_id_str = job_id if job_id else "manual"
        subfolder_str = subfolder_id if subfolder_id else "0"
        output_file = os.path.join(folder_name, f"journal_ID{job_id_str}_{subfolder_str}.txt")
        comando = f'journalctl --since="{start_time}" --until="{end_time}" > "{output_file}"'
        logging.info(f"Comando generado: {comando}")
        print(f"Comando journalctl generado: {comando}")

        return start_time, end_time

    except Exception as e:
        logging.error(f"Error al extraer timestamps: {e}")
        return None, None


def ejecutar_journalctl_remoto(ssh, sftp, ip, start_time, end_time, job_id, folder_name):
    import os

    try:
        logging.info("Ejecutando journalctl remoto...")

        remote_output = f"/tmp/journal_{job_id}.txt"
        local_output = os.path.join(folder_name, f"journal_JobID{job_id}.txt")

        comando = f'journalctl --since="{start_time}" --until="{end_time}" > {remote_output}'
        ssh.exec_command(comando)

        # Esperar a que se genere el archivo (opcional: agregar verificación)
        sftp.get(remote_output, local_output)
        logging.info(f"Archivo `journalctl` descargado desde el servidor remoto: {local_output}")

        ssh.exec_command(f"rm {remote_output}")
        logging.info("Archivo temporal journalctl eliminado del servidor.")

    except Exception as e:
        logging.error(f"Error al ejecutar journalctl remoto: {e}")


def copiar_modulo_rcv_remoto(ip, modulo, run, folder_name):

    def copiar_recursivo(sftp, remote_path, local_path):
        os.makedirs(local_path, exist_ok=True)
        archivos = []

        for item in sftp.listdir_attr(remote_path):
            remote_item = f"{remote_path}/{item.filename}"
            local_item = os.path.join(local_path, item.filename)
            archivos.append((remote_item, local_item, S_ISDIR(item.st_mode)))

        for remote_item, local_item, es_directorio in tqdm(archivos, desc="Copiando archivos", unit="archivo"):
            if es_directorio:
                copiar_recursivo(sftp, remote_item, local_item)
            else:
                sftp.get(remote_item, local_item)

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip, username="root", password="sn1ckers?")
        sftp = ssh.open_sftp()

        base_path = f"/root/utf/logs/{modulo}"
        carpetas = sftp.listdir(base_path)
        carpeta_run = next((c for c in carpetas if c.startswith(f"run_{run}_")), None)

        if not carpeta_run:
            logging.error(f"No se encontró carpeta para run {run} en {base_path}")
            return None, None, None

        remote_path = f"{base_path}/{carpeta_run}"
        copiar_recursivo(sftp, remote_path, folder_name)

        return ssh, sftp, folder_name

    except Exception as e:
        logging.error(f"Error al copiar módulo RCV remoto: {e}")
        return None, None, None


def opcion_3_modulo_rcv():
    ip = input("Ingrese la IP del servidor remoto: ")
    modulo = input("Ingrese el nombre del módulo RCV (ej. utf_rcv_disk_io_fdp_fvd23037): ")
    run = input("Ingrese el número de run (ej. 1): ")
    folder_name = input("Ingrese el nombre de carpeta local destino: ")

    ssh, sftp, ruta_local = copiar_modulo_rcv_remoto(ip, modulo, run, folder_name)
    if not ssh or not sftp or not ruta_local:
        logging.error("No se pudo copiar el módulo remoto.")
        return None, None, None, None, None

    # Extraer timestamps desde UTFManager.log
    start_time, end_time = extraer_timestamps_y_generar_comando(ip, None, None, folder_name)
    if not start_time or not end_time:
        logging.error("No se pudieron extraer los timestamps.")
        return None, None, None, None, None

    # Ejecutar filtrado y journalctl usando la misma sesión
    generar_messages_filtrados_remoto(ssh, sftp, ip, start_time, end_time, folder_name)
    ejecutar_journalctl_remoto(ssh, sftp, ip, start_time, end_time, "RCV", folder_name)

    sftp.close()
    ssh.close()

    return ip, folder_name, ruta_local, start_time, end_time


# -----------------------------------------
# Flujo principal del script
# -----------------------------------------


def main():
    print("\n")
    print("=== RCV Debugging One-Stop Script ===")
    print("Opciones disponibles:")
    print("1. Procesar por Job ID")
    print("2. Procesar por Ticket ID")
    print("3. Conexión remota por IP y módulo RCV")

    opcion = input("Seleccione una opción (1, 2 o 3): ")

    job_id = subfolder_id = ip_address = folder_name = ruta_local = None
    start_time = end_time = None

    if opcion == "1":
        entrada_id = input("Ingrese el Job ID: ")
        subfolder_id = input("Ingrese el Task Index: ")
        folder_name = entrada_id
        ruta_local = copy_network_folder(entrada_id, subfolder_id, folder_name)

        if not ruta_local:
            logging.error("No se pudo copiar la carpeta desde red.")
            return

        ruta_preparada = descomprimir_zip_en_carpeta(ruta_local)

        start_time, end_time = extraer_timestamps_y_generar_comando(None, entrada_id, subfolder_id, ruta_preparada)
        if not start_time or not end_time:
            logging.error("No se pudieron extraer los timestamps.")
            return

        descripcion_path = os.path.join(ruta_preparada, "jira-description.txt")
        if os.path.exists(descripcion_path):
            logging.info(f"Archivo jira-description.txt encontrado en {ruta_preparada}. Extrayendo IP...")
            _, _, ip_address = extract_paths_and_ip_desde_archivo(descripcion_path)
        else:
            logging.warning(f"Archivo jira-description.txt no encontrado en {ruta_preparada}. Intentando fallback...")
            ip_address = None

        if not ip_address:
            logging.error("No se pudo obtener la IP ni desde el archivo ni desde otra fuente.")
            return

        job_id = entrada_id

    elif opcion == "2":
        entrada_id = input("Ingrese el Ticket ID: ")
        usuario, contrasena = fetch_credentials()
        jira = initialize_jira_connection(usuario, contrasena)

        if jira is None:
            logging.error("No se pudo conectar a Jira. Verifica tus credenciales.")
            print("❌ Error de conexión con Jira. Verifica usuario y contraseña.")
            return

        job_id, subfolder_id, ip_address = extract_paths_and_ip(jira, entrada_id)

        if not job_id:
            logging.error("No se encontró Job ID en el ticket.")
            return

        folder_name = entrada_id
        ruta_local = copy_network_folder(job_id, subfolder_id, folder_name)

        if not ruta_local:
            logging.error("No se pudo copiar la carpeta desde red.")
            return

        ruta_preparada = descomprimir_zip_en_carpeta(ruta_local)

        start_time, end_time = extraer_timestamps_y_generar_comando(ip_address, job_id, subfolder_id, ruta_preparada)
        if not start_time or not end_time:
            logging.error("No se pudieron extraer los timestamps.")
            return

    elif opcion == "3":
        job_id = "RCV"
        ip_address, folder_name, ruta_local, start_time, end_time = opcion_3_modulo_rcv()
        if None in (ip_address, folder_name, ruta_local, start_time, end_time):
            print("❌ Error en la opción 3. Verifica IP, módulo o número de run.")
            return
        # Ya se ejecutaron los pasos remotos dentro de opcion_3_modulo_rcv()

    else:
        print("Opción inválida.")
        return

    # 🔐 Establecer sesión SSH una sola vez (solo para opción 1 y 2)
    if opcion in ["1", "2"]:
        ssh, sftp = establecer_conexion_ssh(ip_address)
        if ssh is None or sftp is None:
            logging.error("No se pudo establecer la sesión SSH. Verifique IP, usuario o contraseña.")
            print("❌ Error de conexión SSH. Verifica que la IP y contraseña sean correctas.")
            return

        try:
            generar_messages_filtrados_remoto(ssh, sftp, ip_address, start_time, end_time, folder_name)
            ejecutar_journalctl_remoto(ssh, sftp, ip_address, start_time, end_time, job_id, folder_name)
        except Exception as e:
            logging.error(f"Error durante ejecución remota: {e}")
        finally:
            sftp.close()
            ssh.close()

    # 📁 Procesar carpeta local
    if ruta_local:
        try:
            ruta_preparada = descomprimir_zip_en_carpeta(ruta_local)
            patron_regex = r"drive_access_tracker\\.0x[a-fA-F0-9]+\\.(txt|log|log.aborted)$"
            consolidar_y_filtrar(ruta_preparada, patron_regex)
        except Exception as e:
            logging.error(f"Error al procesar carpeta local: {e}")
    else:
        logging.error("No se pudo procesar la carpeta local porque 'ruta_local' es None.")

if __name__ == "__main__":
    main()
