import os
import time
from typing import Dict, List


def get_weather_data(filepath: str) -> Dict[str, List[float]]:
    results = {}
    file = open(filepath, "r")
    while True:
        line = file.readline()
        if not line:
            break
        line = line.split(';')
        if line:
            if line[0] not in results:
                results[line[0]] = []
            results[line[0]].append(float(line[1]))
    return results

def get_city_stats(weather_data: Dict[str, List[float]]):
    results = ", ".join(f"{city}={min(weather_data[city])}/{round(sum(weather_data[city]) / len(weather_data[city]), 1)}/{max(weather_data[city])}" for city in weather_data)
    return f"{{{results}}}"

if __name__ == '__main__':
    start = time.perf_counter()
    results = get_weather_data("data/my_weather_stations.csv")
    end = time.perf_counter()
    print(f"Completed get_weather_data in {end-start:0.4f} seconds")
    start = time.perf_counter()
    get_city_stats(results)
    end = time.perf_counter()
    print(f"Completed get_city_stats in {end-start:0.4f} seconds")