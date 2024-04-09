from datetime import datetime
from swiftclient.service import SwiftService, SwiftError
import logging


# Configurações de autenticação do OpenStack Swift
auth_url = 'https://auth.s3.globoi.com:5000/v3'
user = '<usuario>'
key = '<senha>'
container_name = '<container>'

# Configurações adicionais para lidar com mais de 10.000 objetos
limit = 10000  # Limite de objetos por solicitação
marker = None  # Marcador inicial

# Função para listar objetos no container e deletar aqueles com mais que X dias
def list_and_delete_old_objects(days_threshold):
    global marker
    with SwiftService(authurl=auth_url, user=user, key=key) as swift:
        try:
            # Loop para lidar com mais de 10.000 objetos
            while True:
                # Listar objetos no container
                list_parts = swift.list(container=container_name, limit=limit, marker=marker)
                logging.info(
                    f'[LIST-PARTS][list_parts] CHECKING LIST PARTS: {list_parts}'
                )

                # Verificar se não há mais objetos
                if not list_parts:
                    break

                # Extrair nomes dos objetos e verificar a data de criação
                for obj in list_parts:
                    object_name = obj['name']
                    creation_date_str = obj['last_modified']
                    creation_date = datetime.strptime(creation_date_str, "%Y-%m-%dT%H:%M:%S.%f")

                    # Calcular a diferença de dias entre a data de criação e a data atual
                    days_difference = (datetime.now() - creation_date).days

                    # Se o objeto tiver mais que X dias, copiá-lo para o container de backup e depois deletá-lo
                    if days_difference > days_threshold:
                        backup_container = "backup_" + container_name
                        # Copiar objeto para o container de backup
                        copy_result = swift.copy(container=backup_container, objects=[object_name], source_container=container_name)
                        if copy_result["successes"]:
                            # Deletar objeto do container original
                            swift.delete(container=container_name, objects=[object_name])
                            logging.info(
                                f'[COPY_RESULT][{object_name} COPIED: {copy_result}]'
                            )
                            logging.info(
                                f'[DELETE_RESULT][{object_name} DELETED: {copy_result}]'
                            )
                        else:
                            logging.error(
                                f'[COPY_RESULT][{object_name} COPIED: FAILED]'
                            )
                # Atualizar marcador para próxima solicitação
                marker = obj['name']

        except SwiftError as e:
            logging.error(
                '[ERROR][Erro ao listar objetos: {e}]'
            )

# Definir o número de dias limite para deletar objetos
dias_limite = 30  # Altere para o número desejado de dias

# Chamada da função para listar e deletar objetos com mais que X dias
list_and_delete_old_objects(dias_limite)