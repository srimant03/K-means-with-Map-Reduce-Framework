import os

def shuffle_and_sort(intermediate_values):
    sorted_intermediate_values = sorted(intermediate_values, key=lambda x: x[0])
    grouped_values = {}

    for key, value in sorted_intermediate_values:
        if key not in grouped_values:
            grouped_values[key] = []
        grouped_values[key].append(value)

    return grouped_values

def reduce_function(centroid_id, grouped_values):
    points = grouped_values[centroid_id]
    new_centroid = [sum(x) / len(points) for x in zip(*points)]
    return centroid_id, new_centroid

def run_reducer(num_mappers, num_reducers, centroid_idx):
    #grpc call to mapper.py
    #to get the intermediate values from all the mappers

    #this is placeholder code to read the intermediate values from files but actually intermediate values will be received from mappers via grpc
    intermediate_values = []
    for i in range(num_mappers):
        mapper_dir = 'mapper_' + str(i)
        with open(mapper_dir + '/partition_' + str(reducer_id) + '.txt', 'r') as f:
            lines = f.readlines()
            for line in lines:
                centroid, point = line.split()
                intermediate_values.append((eval(centroid), eval(point)))

    #shuffle and sort all intermediate values
    grouped_values = shuffle_and_sort(intermediate_values)
    
    #centroid id assigned by master to a reducer task
    #run_reducer is called using a grpc invoked by the master(centroid id is passed at that time)

    new_centroids = [reduce_function(centroid_idx, grouped_values)]

    if not os.path.exists('reducer_' + str(centroid_idx)):
        os.makedirs('reducer_' + str(centroid_idx))

    with open('reducer_' + str(centroid_idx) + '/output.txt', 'w') as f:
        for centroid_id, new_centroid in new_centroids:
            f.write(str(centroid_id) + ' ' + str(new_centroid) + '\n')
    
    #now reply to initial grpc call has to be sent to master containing updated centroid value
    
if __name__ == '__main__':
    
    #grpc call has to accepted here 
    #code for grpc and call to run_reducer


    num_mappers = 2
    num_reducers = 2
    reducer_id = 1
    run_reducer(num_mappers, num_reducers, reducer_id)



    


