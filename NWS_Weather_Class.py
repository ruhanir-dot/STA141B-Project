import requests
import pandas as pd
from datetime import datetime
import time

class NWSWeatherAPI:
    def __init__(self, user_agent="CaliforniaWeatherApp (contact@example.com)"):
        self.base_url = "https://api.weather.gov"
        self.headers = {
            "User-Agent": user_agent,
            "Accept": "application/geo+json"
        }
    def get_point_metadata(self, latitude, longitude):
        # metadata for point location
        url = f"{self.base_url}/points/{latitude},{longitude}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            properties = data.get('properties', {})
            
            return {
                'gridId': properties.get('gridId'),
                'gridX': properties.get('gridX'),
                'gridY': properties.get('gridY'),
                'forecast_url': properties.get('forecast'),
                'forecast_hourly_url': properties.get('forecastHourly'),
                'observation_stations_url': properties.get('observationStations'),
                'timezone': properties.get('timeZone'),
                'city': properties.get('relativeLocation', {}).get('properties', {}).get('city'),
                'state': properties.get('relativeLocation', {}).get('properties', {}).get('state')
            }
        except Exception as e:
            print(f"Error: {e}")
            return None
    
    def get_forecast(self, latitude, longitude, hourly=False):
        # get weather forecast
        metadata = self.get_point_metadata(latitude, longitude)
        if not metadata:
            return None
        
        forecast_url = metadata['forecast_hourly_url'] if hourly else metadata['forecast_url']
        
        try:
            response = requests.get(forecast_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            periods = data.get('properties', {}).get('periods', [])
            df = pd.DataFrame(periods)
            
            if 'startTime' in df.columns:
                df['startTime'] = pd.to_datetime(df['startTime'])
            if 'endTime' in df.columns:
                df['endTime'] = pd.to_datetime(df['endTime'])
            
            # location info
            df['city'] = metadata.get('city')
            df['state'] = metadata.get('state')
            df['latitude'] = latitude
            df['longitude'] = longitude
            
            return df
            
        except Exception as e:
            print(f"Error: {e}")
            return None
    
    def get_observations(self, latitude, longitude, limit=50):
        # recent weather observations
        metadata = self.get_point_metadata(latitude, longitude)
        if not metadata or not metadata.get('observation_stations_url'):
            return None
        
        try:
            # observation stations
            stations_response = requests.get(
                metadata['observation_stations_url'], 
                headers=self.headers, 
                timeout=10
            )
            stations_response.raise_for_status()
            stations_data = stations_response.json()
            
            stations = stations_data.get('features', [])
            if not stations:
                print("No observation stations found")
                return None
            
            # observations from first station
            station_id = stations[0].get('properties', {}).get('stationIdentifier')
            obs_url = f"{self.base_url}/stations/{station_id}/observations"
            
            obs_response = requests.get(
                obs_url, 
                headers=self.headers, 
                params={'limit': limit},
                timeout=10
            )
            obs_response.raise_for_status()
            obs_data = obs_response.json()
            
            observations = []
            for feature in obs_data.get('features', []):
                props = feature.get('properties', {})
                observations.append({
                    'timestamp': props.get('timestamp'),
                    'station': station_id,
                    'temperature_c': props.get('temperature', {}).get('value'),
                    'dewpoint_c': props.get('dewpoint', {}).get('value'),
                    'wind_direction': props.get('windDirection', {}).get('value'),
                    'wind_speed_kmh': props.get('windSpeed', {}).get('value'),
                    'wind_gust_kmh': props.get('windGust', {}).get('value'),
                    'barometric_pressure_pa': props.get('barometricPressure', {}).get('value'),
                    'visibility_m': props.get('visibility', {}).get('value'),
                    'relative_humidity': props.get('relativeHumidity', {}).get('value'),
                    'precipitation_last_hour_mm': props.get('precipitationLastHour', {}).get('value'),
                    'description': props.get('textDescription')
                })
            
            df = pd.DataFrame(observations)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # to Fahrenheit
            df['temperature_f'] = df['temperature_c'] * 9/5 + 32
            df['dewpoint_f'] = df['dewpoint_c'] * 9/5 + 32
            
            return df
            
        except Exception as e:
            print(f"Error: {e}")
            return None
    
    def get_alerts(self, state="CA"):
        # alerts for california
        url = f"{self.base_url}/alerts/active"
        params = {'area': state}
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            alerts = []
            for feature in data.get('features', []):
                props = feature.get('properties', {})
                alerts.append({
                    'event': props.get('event'),
                    'severity': props.get('severity'),
                    'certainty': props.get('certainty'),
                    'urgency': props.get('urgency'),
                    'headline': props.get('headline'),
                    'description': props.get('description'),
                    'instruction': props.get('instruction'),
                    'onset': props.get('onset'),
                    'expires': props.get('expires'),
                    'affected_zones': ', '.join(props.get('affectedZones', []))
                })
            
            df = pd.DataFrame(alerts)
            if not df.empty and 'onset' in df.columns:
                df['onset'] = pd.to_datetime(df['onset'])
                df['expires'] = pd.to_datetime(df['expires'])
            
            return df
            
        except Exception as e:
            print(f"Error: {e}")
            return None