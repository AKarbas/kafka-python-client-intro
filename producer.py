from confluent_kafka import Producer
import yaml


CONFIG_PATH = './config.yaml'


def load_config():
    with open(CONFIG_PATH, mode='r', encoding='utf-8') as config_file:
        return yaml.safe_load(config_file)['Produce']


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def main():
    print('Starting Producer')

    config = load_config()

    p = Producer({
        'bootstrap.servers': ','.join(config['BootstrapServers']),
        'acks': config['Acks']
    })

    for msg in [f'Message #{x}' for x in range(config['MinNum'], config['MaxNum'])]:
        p.poll(0)
        p.produce(config['Topic'], msg.encode('utf-8'),
                  callback=delivery_report)
        print(f'Produced: {msg}')

    p.flush(10.0)


if __name__ == "__main__":
    main()
