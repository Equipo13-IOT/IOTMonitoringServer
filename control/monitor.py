from argparse import ArgumentError
import ssl
from django.db.models import Avg
from datetime import timedelta, datetime, timezone
from receiver.models import Data, Measurement
import paho.mqtt.client as mqtt
import schedule
import time
from django.conf import settings

client = mqtt.Client(settings.MQTT_USER_PUB)


def analyze_data():
    # Consulta todos los datos de la última hora, los agrupa por estación y variable
    # Compara el promedio con los valores límite que están en la base de datos para esa variable.
    # Si el promedio se excede de los límites, se envia un mensaje de alerta.

    print("Calculando alertas...")

    check_min_max_alert()
    check_temperature_close_to_average()


def check_min_max_alert():
    data = Data.objects.filter(
        base_time__gte=datetime.now() - timedelta(hours=1))
    aggregation = data.annotate(check_value=Avg('avg_value')) \
        .select_related('station', 'measurement') \
        .select_related('station__user', 'station__location') \
        .select_related('station__location__city', 'station__location__state',
                        'station__location__country') \
        .values('check_value', 'station__user__username',
                'measurement__name',
                'measurement__max_value',
                'measurement__min_value',
                'station__location__city__name',
                'station__location__state__name',
                'station__location__country__name')
    alerts = 0
    for item in aggregation:
        alert = False

        variable = item["measurement__name"]
        max_value = item["measurement__max_value"] or 0
        min_value = item["measurement__min_value"] or 0

        country = item['station__location__country__name']
        state = item['station__location__state__name']
        city = item['station__location__city__name']
        user = item['station__user__username']

        if item["check_value"] > max_value or item["check_value"] < min_value:
            alert = True

        if alert:
            message = "ALERT {} {} {}".format(variable, min_value, max_value)
            topic = '{}/{}/{}/{}/in'.format(country, state, city, user)
            print(datetime.now(), "Sending alert to {} {}".format(topic, variable))
            client.publish(topic, message)
            alerts += 1
    print(len(aggregation), "dispositivos revisados")
    print(alerts, "alertas enviadas")


from datetime import datetime, timedelta


def check_temperature_close_to_average():
    # Define the percentage threshold for being "close to average" (e.g., 5%)
    threshold_percentage = 5  # Adjust this value as needed

    # Get data from the last hour using the initial query
    data = Data.objects.filter(
        base_time__gte=datetime.now() - timedelta(hours=1)  # Use datetime.now()
    )

    # Aggregate and annotate with the average value for the last hour
    aggregation = data.annotate(check_value=Avg('avg_value')) \
        .select_related('station', 'measurement') \
        .select_related('station__user', 'station__location') \
        .select_related('station__location__city', 'station__location__state', 'station__location__country') \
        .values('check_value', 'station', 'station__user__username', 'measurement__name',
                'station__location__city__name', 'station__location__state__name', 'station__location__country__name')

    alerts = 0
    for item in aggregation:
        # Fetch the latest reading (already covered in the previous aggregation)
        latest_reading = Data.objects.filter(
            station=item['station'],
            measurement__name=item['measurement__name']
        ).order_by('-base_time').values_list('avg_value', flat=True).first()

        # Skip if no latest reading is available
        if latest_reading is None:
            continue

        # Get the average value from the aggregation
        average_value = item["check_value"]

        # Calculate the allowable range around the average
        lower_bound = average_value * (1 - threshold_percentage / 100.0)
        upper_bound = average_value * (1 + threshold_percentage / 100.0)

        # Check if the latest reading is within the threshold range of the average
        if lower_bound <= latest_reading <= upper_bound:
            # If the latest reading is close to the average, prepare to send an alert
            variable = item["measurement__name"]
            country = item['station__location__country__name']
            state = item['station__location__state__name']
            city = item['station__location__city__name']
            user = item['station__user__username']

            # Create the alert message
            message = f"Temperature close to average: {latest_reading:.2f} (Average: {average_value:.2f})"

            # Construct the MQTT topic
            topic = f'{country}/{state}/{city}/{user}/temperature_close_to_average'

            # Publish the alert to the MQTT broker
            client.publish(topic, message)
            print(datetime.now(), f"Sending alert to {topic}: {message}")
            alerts += 1

    print(f"{len(aggregation)} devices checked")
    print(f"{alerts} alerts sent")


def on_connect(client, userdata, flags, rc):
    '''
    Función que se ejecuta cuando se conecta al bróker.
    '''
    print("Conectando al broker MQTT...", mqtt.connack_string(rc))


def on_disconnect(client: mqtt.Client, userdata, rc):
    '''
    Función que se ejecuta cuando se desconecta del broker.
    Intenta reconectar al bróker.
    '''
    print("Desconectado con mensaje:" + str(mqtt.connack_string(rc)))
    print("Reconectando...")
    client.reconnect()


def setup_mqtt():
    '''
    Configura el cliente MQTT para conectarse al broker.
    '''

    print("Iniciando cliente MQTT...", settings.MQTT_HOST, settings.MQTT_PORT)
    global client
    try:
        client = mqtt.Client(settings.MQTT_USER_PUB)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect

        if settings.MQTT_USE_TLS:
            client.tls_set(ca_certs=settings.CA_CRT_PATH,
                           tls_version=ssl.PROTOCOL_TLSv1_2, cert_reqs=ssl.CERT_NONE)

        client.username_pw_set(settings.MQTT_USER_PUB,
                               settings.MQTT_PASSWORD_PUB)
        client.connect(settings.MQTT_HOST, settings.MQTT_PORT)

    except Exception as e:
        print('Ocurrió un error al conectar con el bróker MQTT:', e)


def start_cron():
    '''
    Inicia el cron que se encarga de ejecutar la función analyze_data cada 5 minutos.
    '''
    print("Iniciando cron...")
    schedule.every(5).minutes.do(analyze_data)
    print("Servicio de control iniciado")
    while 1:
        schedule.run_pending()
        time.sleep(1)
