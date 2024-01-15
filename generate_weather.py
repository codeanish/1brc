import time
from typing import List, Tuple
import random

def get_cities(filepath: str) -> List[str]:
    cities = []
    with open(filepath, "r") as file:
        # Skip the first line
        file.readline()
        while True:
            line = file.readline()
            if not line:
                break
            line = line.split(',')
            if line:
                cities.append(line[0].strip('"'))
    return cities


def generate_weather_for_cities(cities: List[str], rows_to_generate: int) -> List[Tuple[str, float]]:
    weather = []
    len_cities = len(cities)
    for i in range(rows_to_generate):
        weather.append((cities[i % len_cities], round(random.uniform(-10,50), 1)))
    return weather


def write_weather_to_file(cities: List[str], rows_to_generate: int, filepath: str) -> None:
    len_cities = len(cities)
    with open(filepath, "w") as file:
        for i in range(rows_to_generate):
            file.write(f"{cities[i % len_cities]};{round(random.uniform(-10, 50), 1)}\n")


if __name__ == "__main__":
    ROWS_TO_GENERATE = 1000000
    cities = get_cities("data/worldcities.csv")
    start = time.perf_counter()
    write_weather_to_file(cities, ROWS_TO_GENERATE, "data/weather_stations.csv")
    end = time.perf_counter()
    print(f"Completed writing {ROWS_TO_GENERATE} write_weather_to_file in {end-start:0.4f} seconds")