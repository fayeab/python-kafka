import logging
import faust
from config import config
from utils import (get_stop_point_by_name, get_next_passage_stop_point, get_token)


logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
app = faust.App('app-missions', broker="kafka://" + config['kafka_brocker'])
topic = app.topic('missionsidfmob')
name, stop_type = 'Les Halles', 'Station de métro'

@app.timer(interval=180)
async def send_message(app):
    token = get_token()
    stop_points = get_stop_point_by_name(name, stop_type)
    missions = []
    for stop_point in stop_points:
        missions.extend(get_next_passage_stop_point(stop_point=stop_point,
                                                    token=token))
    for mission in missions:
        await topic.send(value=mission)
        logging.info(f"Input data : {mission['RecordedAtTime']} - {mission['LineRef']}")

if __name__ == '__main__':
    logging.info(' Starting application ...')
    app.main()