
import time
from typing import Dict, List


def get_weather_data(filepath: str) -> Dict[str, List[float]]:
    results = {}
    with open(filepath, "r") as file:
        for line in file:
            location, measurement = line.split(';')
            if results.get(location):
                results[location].append(float(measurement))
            else:
                results[location] = [float(measurement)]
    return results

if __name__ == "__main__":
    start = time.perf_counter()
    get_weather_data("data/small_weather_stations.csv")
    
    end = time.perf_counter()
    print(f"Completed in {end-start:0.4f} seconds")
    # print(sum(simple_get_weather_data("data/tokyo.csv")['Tokyo']))
    # print(len(simple_get_weather_data("data/tokyo.csv")['Tokyo']))