import os

def map_function(input_split, centroids):
    #for each point in the input_split, find the nearest centroid and emit (centroid, point)
    result = []
    for point in input_split:
        min_dist = float('inf')
        nearest_centroid = None
        for centroid in centroids:
            dist = sum([(a - b) ** 2 for a, b in zip(point, centroid)]) ** 0.5
            if dist < min_dist:
                min_dist = dist
                nearest_centroid = centroid
        result.append((nearest_centroid, point))
    return result

def partition(mapped_values, num_reducers, mapper_id):
    #partition the mapped_values to num_reducers partitions
    partitions = [[] for _ in range(num_reducers)]
    for centroid, point in mapped_values:
        partitions[hash(centroid) % num_reducers].append((centroid, point))
    
    mapper_dir = 'mapper_' + str(mapper_id)
    if not os.path.exists(mapper_dir):
        os.makedirs(mapper_dir)
    for i, partition in enumerate(partitions):
        with open(mapper_dir + '/partition_' + str(i) + '.txt', 'w') as f:
            for centroid, point in partition:
                f.write(str(centroid) + ' ' + str(point) + '\n')

def run_mapper(input_split, centroids, num_reducers, mapper_id):
    mapped_values = map_function(input_split, centroids)
    partition(mapped_values, num_reducers, mapper_id)

    #after running partition function, each mapper must return success to master then master will call reducer

    #add code for grpc/sending reply(success) to master

if __name__ == '__main__':
    
    #accept grpc call from master here
    #code for grpc and call to run_mapper 
    input_split = [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]]
    centroids = [[1, 2], [3, 4]]
    num_reducers = 2
    mapper_id = 1
    run_mapper(input_split, centroids, num_reducers, mapper_id)
