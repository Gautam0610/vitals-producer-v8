
import os
import time
import random
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Load environment variables
output_topic = os.environ.get("OUTPUT_TOPIC")
bootstrap_servers = os.environ.get("BOOTSTRAP_SERVERS")
sasl_username = os.environ.get("SASL_USERNAME")
sasl_password = os.environ.get("SASL_PASSWORD")
interval_ms = int(os.environ.get("INTERVAL_MS", 1000))

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    sasl_plain_username=sasl_username,
    sasl_plain_password=sasl_password,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    ssl_check_hostname=True,
    value_serializer=lambda v: str(v).encode('utf-8')
)


def generate_vitals_data():
    body_temp = round(random.uniform(36.0, 39.0), 1)  # 36.0-39.0 Celsius
    heart_rate = random.randint(60, 100)  # 60-100 bpm
    systolic_pressure = random.randint(90, 140)  # 90-140 mmHg
    diastolic_pressure = random.randint(60, 90)  # 60-90 mmHg
    breaths_per_minute = random.randint(12, 20)  # 12-20 breaths/min
    oxygen_saturation = random.randint(95, 100)  # 95-100%
    blood_glucose = random.randint(70, 140)  # 70-140 mg/dL

    vitals_data = {
        "body_temp": body_temp,
        "heart_rate": heart_rate,
        "systolic_pressure": systolic_pressure,
        "diastolic_pressure": diastolic_pressure,
        "breaths_per_minute": breaths_per_minute,
        "oxygen_saturation": oxygen_saturation,
        "blood_glucose": blood_glucose
    }
    return vitals_data


# Main loop to send data continuously
if __name__ == "__main__":
    while True:
        vitals = generate_vitals_data()
        try:
            producer.send(output_topic, value=vitals)
            print(f"Sent: {vitals}")
            producer.flush() # Ensure message is sent immediately
        except KafkaError as e:
            print(f"Error sending message: {e}")
        time.sleep(interval_ms / 1000)
