import mysql.connector
from pathlib import Path
from config import ZONES_CONFIG_FILEPATH, DIR_FOR_ZONES_FILES
from db_config import *


def create_zone_file(zones_and_hosts: dict, files_dir_path: Path):
    """
    Создание файлов с описанием хостов каждой зоны
    :param a_records:
    :param file_path:
    :return:
    """
    files_dir_path.mkdir(parents=True, exist_ok=True)
    for file in files_dir_path.glob('*'):
        try:
            file.unlink()
        except OSError as err:
            print(f'Ошибка при удалении файла {file}: {err.strerror}')

    file_contents = f"""@ 86400 IN      SOA     srv2.smcit.ru root.smcit.ru. (
                              4         ; Serial
                         604800         ; Refresh
                          86400         ; Retry
                        2419200         ; Expire
                         604800 )       ; Negative Cache TTL
@       IN      NS      srv2.smcit.ru.\n"""
    zones = zones_and_hosts.keys()
    for zone in zones:
        with open(Path(files_dir_path, zone + '.db'), 'w') as file:
            file.write(file_contents)
            for host, ip in zones_and_hosts.get(zone).items():
                name = host.split(".")[0]
                file.write(f"{name}\tIN\tA\t{ip}\n")


def get_local_zones(filepath: Path) -> str:
    """
    Считываем данные о локальных зонах, для их повторной записи в файл
    :param filepath: путь к файлу
    :return: Строка с данными о локальных зонах
    """
    with open(filepath, 'r') as file:
        file_content = file.read()
        split_str = '# Medical organizations zones'
        return file_content.split(split_str)[0] + split_str + '\n\n'


def get_data_from_db() -> list:
    """
    Получаем данные о DNS именах и их ip адресах из БД
    :return: Список вида: [dns_name, ip_address]
    """

    cnx = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD,
                                  host=DB_HOST,
                                  database=DB_BASENAME)
    cursor = cnx.cursor()
    cursor.execute("SELECT ms.hostname, ms.ipv4 from misinfo_servers as ms "
                   "JOIN misinfo_mo as mo ON ms.mo_id = mo.id WHERE ms.poweron = 1 AND mo.support = 1;")
    data = cursor.fetchall()
    cursor.close()
    cnx.close()
    return data


def get_zones_and_hosts_from_db_data(data: list) -> dict:
    """
    Формируем словарь вида {zone: {hostname1: ip_address1, hostname2: ipaddress2}, ...}
    :return: Словарь с данными о зонах, именах и адресах
    """
    zones = dict()
    for domen, ip in data:
        zone = domen.split(".")[-2:]
        zone = ".".join(zone)
        if zone in zones.keys():
            zones[zone].update({domen: ip})
        else:
            zones[zone] = {domen: ip}
    sorted(zones)
    return zones


def write_data_to_zones_conf_file(filename: Path, zones: dict):
    """
    Пишем информацию о локальных зонах и полученную информацию о зонах из БД в файл
    :return:
    """
    local_zones = get_local_zones(filename)
    with open(filename, 'w') as file:
        file.write(local_zones)
        for zone in zones.keys():
            file_zone_path = f'{DIR_FOR_ZONES_FILES}/{zone}.db'
            file.write(f"""zone "{zone}" {{
                type master;
                file "{file_zone_path}";
        }};\n\n""")


if __name__ == "__main__":
    data = get_data_from_db()
    zones_and_hosts = get_zones_and_hosts_from_db_data(data)
    write_data_to_zones_conf_file(ZONES_CONFIG_FILEPATH, zones_and_hosts)
    create_zone_file(zones_and_hosts, DIR_FOR_ZONES_FILES)