import yaml
import os

CONFIG_DIR = os.path.dirname(__file__)

with open(os.path.join(CONFIG_DIR, "config.yaml")) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)