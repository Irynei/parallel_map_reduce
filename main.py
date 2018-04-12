import math
import random
import multiprocessing
from functools import reduce


def sequential_map_reduce(input_data):
    mapped = map(lambda x: (x, 1), input_data)
    combined_data = {}
    for item in mapped:
        try:
            combined_data[item[0]].append(item[1])
        except KeyError:
            combined_data[item[0]] = [item[1]]
    return [reduce(lambda x, y: (x, sum(y)), b) for b in combined_data.items()]


def mapper(shared_array, arr, core_num):
    """ Performs map x -> (x, 1). Stores result in shared array """
    combined_data = tuple((i, 1) for i in arr)
    shared_array[core_num] = combined_data


def reducer(intermediate_results, return_list, keys, core_num):
    """
    Reducer collects data from intermediate results by keys and then performs reduce.
    Stores result in shared array.
    """
    # combine data
    reduced_data = {}
    for item in intermediate_results:
        if item[0] in keys:
            try:
                reduced_data[item[0]].append(item[1])
            except KeyError:
                reduced_data[item[0]] = [item[1]]
    # reduce
    for i in reduced_data:
        reduced_data[i] = sum(reduced_data[i])

    return_list[core_num] = reduced_data


def _get_number_of_reducers_and_chunk_size(all_mapped_keys, number_of_reducers):
    """ Returns number of reducers and number of keys per reducer. """
    keys_per_reducer = math.ceil(len(all_mapped_keys) / number_of_reducers)
    return min(math.ceil(len(all_mapped_keys) / keys_per_reducer), max_cores), keys_per_reducer


def _get_number_of_mappers_and_chunk_size(input_size, max_cores):
    """ Returns number of mappers and chunk size (number of elements per map)"""
    chunk_size = 1
    core_number = input_size // chunk_size
    if core_number > max_cores:
        core_number = max_cores
        chunk_size = int(input_size / core_number)
    return core_number, chunk_size


if __name__ == '__main__':
    max_cores = 4
    input_size = 2**10
    data = [random.randint(0, 9) for _ in range(input_size)]
    # print("Input list:", data)

    jobs = []
    # map phase
    manager = multiprocessing.Manager()
    mappers, map_chunk_size = _get_number_of_mappers_and_chunk_size(input_size, max_cores)
    return_list_mappers = manager.list([0] * mappers)
    print("Number of mappers:", mappers)
    for i in range(mappers):
        start = i * map_chunk_size
        end = start + map_chunk_size
        p = multiprocessing.Process(target=mapper, args=(return_list_mappers, data[start:end], i))
        p.daemon = False
        jobs.append(p)
        p.start()
    for p in jobs:
        p.join()

    # collect intermediate result from mappers
    intermediate_map_result = [item for sublist in return_list_mappers for item in sublist]
    # print("Intermediate result from mappers:", intermediate_map_result)
    all_mapped_keys = list(set(i[0] for i in intermediate_map_result))

    # reduce phase
    number_of_reducers, reduce_chunk_size = _get_number_of_reducers_and_chunk_size(all_mapped_keys, max_cores)
    return_list_reducers = manager.list([0] * number_of_reducers)
    print("Number of reducers:", number_of_reducers)
    for i in range(number_of_reducers):
        start = reduce_chunk_size * i
        end = start + reduce_chunk_size
        keys_per_core = all_mapped_keys[start: end]
        p = multiprocessing.Process(
            target=reducer, args=(intermediate_map_result, return_list_reducers, all_mapped_keys[start: end], i)
        )
        p.daemon = False
        jobs.append(p)
        p.start()
    for p in jobs:
        p.join()

    # combine reduce results
    final_result = {}
    for i in return_list_reducers:
        final_result.update(i)
    print("Result: ", final_result)
