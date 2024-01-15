import os
import time
from typing import Dict, List
import multiprocessing as mp

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

def get_file_chunks(
    file_name: str,
    max_cpu: int = 8,
) -> list:
    """Split flie into chunks"""
    cpu_count = min(max_cpu, mp.cpu_count())

    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_count

    start_end = list()
    with open(file_name, "r+b") as f:

        def is_new_line(position):
            if position == 0:
                return True
            else:
                f.seek(position - 1)
                return f.read(1) == b"\n"

        def next_line(position):
            f.seek(position)
            f.readline()
            return f.tell()

        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)

            while not is_new_line(chunk_end):
                chunk_end -= 1

            if chunk_start == chunk_end:
                chunk_end = next_line(chunk_end)

            start_end.append(
                (
                    file_name,
                    chunk_start,
                    chunk_end,
                )
            )

            chunk_start = chunk_end

    return start_end

def get_city_stats(weather_data: Dict[str, List[float]]):
    results = ", ".join(f"{city}={min(weather_data[city])}/{round(sum(weather_data[city]) / len(weather_data[city]), 1)}/{max(weather_data[city])}" for city in weather_data)
    return f"{{{results}}}"

def _process_file_chunk(
    file_name: str,
    chunk_start: int,
    chunk_end: int,
) -> dict:
    """Process each file chunk in a different process"""
    result = dict()
    with open(file_name, "r") as f:
        f.seek(chunk_start)
        for line in f:
            chunk_start += len(line)
            if chunk_start > chunk_end:
                break
            location, measurement = line.split(";")
            measurement = float(measurement)
            if location not in result:
                result[location] = [
                    measurement,
                    measurement,
                    measurement,
                    1,
                ]  # min, max, sum, count
            else:
                if measurement < result[location][0]:
                    result[location][0] = measurement
                if measurement > result[location][1]:
                    result[location][1] = measurement
                result[location][2] += measurement
                result[location][3] += 1
    return result

def process_file(
    cpu_count: int,
    chunks: list,
) -> None:
    """Process data file"""
    with mp.Pool(cpu_count) as p:
        # Run chunks in parallel
        chunk_results = p.starmap(
            _process_file_chunk,
            chunks,
        )

    # Combine all results from all chunks
    result = dict()
    for chunk_result in chunk_results:
        for location, measurements in chunk_result.items():
            if location not in result:
                result[location] = measurements
            else:
                _result = result[location]
                if measurements[0] < _result[0]:
                    _result[0] = measurements[0]
                if measurements[1] > _result[1]:
                    _result[1] = measurements[1]
                _result[2] += measurements[2]
                _result[3] += measurements[3]

    # Print final results
    print("{", end="")
    for location, measurements in sorted(result.items()):
        print(
            f"{location}={measurements[0]:.1f}/{(measurements[2] / measurements[3]) if measurements[3] !=0 else 0:.1f}/{measurements[1]:.1f}",
            end=", ",
        )
    print("\b\b} ")

if __name__ == '__main__':

    start = time.perf_counter()
    chunks = get_file_chunks("data/weather_stations.csv", 10)
    process_file(10, chunks)
    end = time.perf_counter()
    print(f"Completed in {end-start:0.4f} seconds")
    