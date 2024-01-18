import multiprocessing as mp
import os
import time

def get_file_chunks(file_name: str, cpu_cores: int):
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_cores
    chunks = []

    with open(file_name, "r") as file:
        chunk_start = 0
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)
            file.seek(chunk_end)
            file.readline()
            chunk_end = file.tell()
            chunks.append((file_name, chunk_start, chunk_end))
            chunk_start = chunk_end
    return chunks

def process_file(cpu_count: int, chunks: list):
    with mp.Pool(cpu_count) as p:
        # Run chunks in parallel
        chunk_results = p.starmap(
            _process_file_chunk,
            chunks,
        )
    # Combine results
    results = {} # {location: [min, max, sum, count]}
    for chunk_result in chunk_results:
        for location, measurements in chunk_result.items():
            if location not in results:
                results[location] = measurements
            else:
                if measurements[0] < results[location][0]:
                    results[location][0] = measurements[0]
                if measurements[1] > results[location][1]:
                    results[location][1] = measurements[1]
                results[location][2] += measurements[2]
                results[location][3] += measurements[3]
    return results

def _process_file_chunk(file_name: str, start: int, end: int) -> dict:
    results = {} # {location: [min, max, sum, count]}
    with open(file_name, "r") as file:
        file.seek(start)
        for line in file:
            start += len(line)
            if start > end:
                break
            location, measurement = line.split(';')
            measurement = float(measurement)
            if results.get(location):
                # If less than min, replace min
                if measurement < results[location][0]:
                    results[location][0] = measurement
                # If more than max, replace max
                if measurement > results[location][1]:
                    results[location][1] = measurement
                # Add to sum
                results[location][2] += measurement
                # Add to count
                results[location][3] += 1
            else:
                results[location] = [measurement, measurement, measurement, 1]
    return results

if __name__ == "__main__":
    chunks = get_file_chunks("data/weather_stations.csv", 10)
    start = time.perf_counter()
    results = process_file(10, chunks)
    end = time.perf_counter()
    print(f"Completed in {end-start:0.4f} seconds")