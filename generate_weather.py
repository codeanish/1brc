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

def write_weather_to_file(weather: List[Tuple[str, float]], filepath: str) -> None:
    with open(filepath, "w") as file:
        for city, temperature in weather:
            file.write(f"{city};{temperature}\n")

if __name__ == "__main__":
    ROWS_TO_GENERATE = 1000000
    cities = get_cities("data/worldcities.csv")
    weather = generate_weather_for_cities(cities, ROWS_TO_GENERATE)
    write_weather_to_file(weather, "data/my_weather_stations.csv")
