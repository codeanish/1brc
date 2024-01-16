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
    results = {}
    for chunk_result in chunk_results:
        for location, measurements in chunk_result.items():
            if location not in results:
                results[location] = measurements
            else:
                results[location].extend(measurements)
    return results

def _process_file_chunk(file_name: str, start: int, end: int):
    results = {}
    with open(file_name, "r") as file:
        file.seek(start)
        for line in file:
            start += len(line)
            if start > end:
                break
            location, measurement = line.split(';')
            if results.get(location):
                results[location].append(float(measurement))
            else:
                results[location] = [float(measurement)]
    return results

if __name__ == "__main__":
    chunks = get_file_chunks("data/small_weather_stations.csv", 10)
    start = time.perf_counter()
    results = process_file(10, chunks)
    end = time.perf_counter()
    print(f"Completed in {end-start:0.4f} seconds")