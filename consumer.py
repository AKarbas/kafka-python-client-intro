from confluent_kafka import Consumer, KafkaError
import yaml


CONFIG_PATH = './config.yaml'


def load_config():
    with open(CONFIG_PATH, mode='r', encoding='utf-8') as config_file:
        return yaml.safe_load(config_file)['Consume']


def main():
    print('Starting Consumer; press Ctrl+C to exit.')

    config = load_config()
    print(f'Config is: {config}')

    c = Consumer({
        'bootstrap.servers': ','.join(config['BootstrapServers']),
        'group.id': config.get('GroupId', 'SomeId'),
        'auto.offset.reset': config.get('AutoOffsetReset', 'latest')
    })
    c.subscribe([config['Topic']])
    while True:
        msg = c.poll(1.0)

        if msg is None:
            print('Polled and got nothing...')
            continue
        if msg.error():
            print(f'Consumer error: {msg.error()}')
            continue
        print(f'Got message: {msg.value().decode("utf-8")}')


if __name__ == "__main__":
    main()
