import grpc
import master_pb2
import master_pb2_grpc
import multiprocessing
import random

class Master(master_pb2_grpc.Master):
    def __init__(self, M, R, K, iter):
        self.m = M
        self.r = R
        self.k = K
        self.iter = iter
        self.input = self.read_input()
        self.centroids = []

    def read_input(self):
        with open('points.txt', 'r') as f:
            lines = f.readlines()
            points = []
            for line in lines:
                points.append([float(x) for x in line.split()])
        return points
    
    def assign_k_centroids(self):
        #from the input points, randomly select k points as centroids and append to self.centroids
        self.centroids = random.sample(self.input, self.k)
        return self.centroids
    
    def centroid_compilation(self, output):
        #read the output from all the reducers and compile the new centroids
        #also update centroids.txt with the new centroids
        pass
    
    def run(self):
        centroids = self.assign_k_centroids()
        print("Initial centroids: ", centroids)
        #update the file centroids.txt with the initial centroids
        with open('centroids.txt', 'w') as f:
            for centroid in centroids:
                f.write(' '.join([str(x) for x in centroid]) + '\n')
          
        for i in range(self.iter):
            #divide the input points into m parts
            parts = [self.input[i::self.m] for i in range(self.m)]

            #code for spawning m processes each a mapper .....
            #grpc call to spawn m mappers

            #after all mappers have finished/returned success, call the reducers

            #code for spawning r processes each a reducer .....
            #grpc call to spawn r reducers
                

def serve():
    M = input("Enter the number of mappers: ")
    R = input("Enter the number of reducers: ")
    K = input("Enter the number of clusters: ")
    iter = input("Enter the number of iterations: ")
    
    #placeholder grpc like code
    server = grpc.server(multiprocessing.ThreadPoolExecutor())
    master_pb2_grpc.add_MasterServicer_to_server(Master(M, R, K, iter), server)
    server.add_insecure_port('[::]:5001')
    server.start()
    server.wait_for_termination()
    
if __name__ == '__main__':
    serve()





