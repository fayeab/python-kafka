import logging
import requests
import geopandas as gpd
from config import config

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)

def get_token():
    data = dict(grant_type='client_credentials',
                scope='read-data',
                client_id=config['client_id'],
                client_secret=config['client_secret'])
    response = requests.post(config['url_oauth'], data=data)
    if response.status_code != 200:
        logging.info('Status:', response.status_code, 'Erreur sur la requête')
        return None

    jsonData = response.json()
    
    return jsonData['access_token']


def get_next_passage_stop_point(stop_point, token=None):

    _token = get_token() if not token else token

    params = dict(MonitoringRef=f'STIF:StopPoint:Q:{stop_point}:')
    headers = {'Accept-Encoding' : 'gzip', 'Authorization' : 'Bearer ' + _token}
    response = requests.get(config['url_stop_monitoring'],
                            params=params,
                            headers=headers)
    if response.status_code != 200:
        logging.info('Status:', response.status_code)
        return []
    
    jsonData = response.json()
    data = jsonData.get('Siri',
                        {}).get('ServiceDelivery', 
                        {}).get('StopMonitoringDelivery', 
                        [{}])[0].get('MonitoredStopVisit', [])
    ans = []
    for resp in data:
        ans.append({
            'RecordedAtTime' : resp.get('RecordedAtTime'),
            'MonitoringRef' : resp.get('MonitoringRef', 
                                        {}).get('value'),
            'MonitoredVehicleJourney' : resp.get('LineRef', 
                                                    {}).get('value'),
            'MonitoringRef' : resp.get('MonitoringRef', 
                                        {}).get('value'),
            'LineRef' : resp.get('MonitoredVehicleJourney', 
                                    {}).get('LineRef', {}).get('value'),
            'ExpectedArrivalTime' : resp.get('MonitoredVehicleJourney',
                                                {}).get('MonitoredCall',
                                                        {}).get('ExpectedArrivalTime'),
            'ExpectedDepartureTime' : resp.get('MonitoredVehicleJourney',
                                    {}).get('MonitoredCall', 
                                            {}).get('ExpectedDepartureTime'),
            'DepartureStatus' : resp.get('MonitoredVehicleJourney',
                                            {}).get('MonitoredCall', 
                                                    {}).get('DepartureStatus'),
            'DirectionName' : resp.get('MonitoredVehicleJourney',
                                            {}).get('DirectionName', 
                                                    [{}])[0].get('value'),
            'StopPointName' : resp.get('MonitoredVehicleJourney',
                                            {}).get('MonitoredCall', 
                                                    {}).get('StopPointName', [{}])[0].get('value')
            })
    
    return ans

def get_stop_point_by_name(name, stop_type):
    list_type_arret = ['Arrêt de bus', 'Arrêt de tram', 'Station de métro', 'Station ferrée / Val']
    if stop_type not in list_type_arret:
        raise Exception(f"stop_type argument must be in {list_type_arret}")
    nom = name.lower()
    type_arret = stop_type.lower()
    data = gpd.read_file('../data/REF_ZDE.zip')
    data['type_arret'] = data['type_arret'].map(lambda x: x.lower())
    data['nom'] = data['nom'].map(lambda x: x.lower())
    res = data.id_refa[(data.type_arret == type_arret) & 
                       (data.nom == nom)].tolist()
    return res


if __name__ == '__main__':
    logging.info('Test function get_next_passage_stop_point')
    from pprint import pprint
    token = get_token()
    stop_points = get_stop_point_by_name('Les Halles', 'Station de métro')
    res = []
    for stop_point in stop_points:
        res.append(get_next_passage_stop_point(stop_point=stop_point, token=token))
    pprint(res)
    
