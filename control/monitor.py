from argparse import ArgumentError
import ssl
from django.db.models import Avg
from datetime import timedelta, datetime
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
    check_stable_temperature()


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


def check_stable_temperature():
    # Fetch all stations
    stations = Data.objects.values_list('station', flat=True).distinct()

    stable_stations = []

    for station in stations:
        # Get the last 4 temperature readings for this station
        readings = Data.objects.filter(station=station, measurement__name='temperature') \
                       .order_by('-base_time')[:4]

        # If we don't have exactly 4 readings, skip this station
        if len(readings) < 4:
            print(f"Station {station} skipped, not enough readings.")
            continue

        # Extract the avg_value from each reading
        temperatures = [reading.avg_value for reading in readings]
        print(f"Station {station}: Last 4 temperatures = {temperatures}")

        # Define what "alike" means, e.g., all temperatures within a range of 2 degrees
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        temp_range = max_temp - min_temp
        print(f"Station {station}: Max temperature = {max_temp}, Min temperature = {min_temp}, Range = {temp_range}")

        if temp_range <= 2:  # You can adjust this range
            stable_stations.append(station)

            # Fetch station details for constructing the topic
            station_details = readings[0].station
            user = station_details.user.username
            country = station_details.location.country.name
            state = station_details.location.state.name
            city = station_details.location.city.name

            # Create the alert message
            message = f"Temperature is stable for the last 4 readings: {temperatures}"

            # Construct the MQTT topic
            topic = f'{country}/{state}/{city}/{user}/stable_temperature'

            # Publish the alert to the MQTT broker
            client.publish(topic, message)
            print(f"Alert sent to {topic}: {message}")
        else:
            print(f"Station {station}: Temperature not stable (Range > 2 degrees)")

    print(f"{len(stable_stations)} stations have stable temperatures.")


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
