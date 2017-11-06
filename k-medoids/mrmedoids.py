
from mrjob.job import MRJob
import math
import sys

class MRKMedoids(MRJob):
    medoids = []
    new_medoids = []
    _correct = 0
    _total = 0

    #this needs to be done because python dont have static vars requires declaration as property and getters and setters
    @property
    def correct(self):
        return self._correct
    
    @correct.setter
    def correct(self,val):
        self._correct = val

    @property
    def total(self):
        return self._total
    
    @total.setter
    def total(self,val):
        self._total = val

    
    #Standard config of variables 
    def configure_options(self):
        
        super(MRKMedoids,self).configure_options()

        self.add_passthrough_option(
            '--iterations', dest='iterations',default=10,type='int',
            help='number of iterations to run')

        self.add_passthrough_option(
            '--c', dest='medoid_file',default='medoids.txt',type='str',
            help='The path to the centroid file')

    #Static methods
    @classmethod
    def get_medoids(self):
        medoids = []
        with open('medoids.txt','r') as f:
            for line in f:
                a, b, c, d, e = line.split(',')
                medoids.append((float(a), float(b), float(c), float(d), e))
        self.medoids = medoids
    
    @classmethod
    def get_correct_percentage(self):
        x = self.correct
        y = self.total
        return (int(x)/int(y))*100

    @classmethod
    def write_medoids(self):
        with open('medoids.txt',"w") as f:
            for medoid in self.new_medoids:
                f.write(','.join([str(x) for x in medoid]) + "\n")

    def calculate_distance(self,att1, att2):
        try:
            (a1, b1, c1, d1) = att1[:4]
            (a1, b1, c1, d1) = (float(a1), float(b1), float(c1), float(d1))
            (a2, b2, c2, d2) = att2[:4]
            else:
                return math.sqrt(math.pow((a1-a2), 2) +
                                math.pow((b1-b2), 2) +
                                math.pow((c1-c2), 2) +
                                math.pow((d1-d2), 2))
    #Calculate distances and classify each point
    def map_task(self, _, line):
        line = line.strip()
        params = line.split(',')
        attributes = [a, b, c ,d] = params[:4]
        attributes = [float(x) for x in attributes]
        dist = []
        for medoid in self.medoids:
            dist.append(self.calculate_distance(medoid[:4],attributes))
            cluster = dist.index(min(dist))
        if self.medoids[cluster][4] == params[4]:
            self.correct += 1
        self.total += 1
        yield cluster, params
    #Calculated distance from each medoid to other medoid ans select one with least cost
    def combine_task(self, cluster, values):
        values = list(values)
        for value in values:
            distTemp = 0
            minDist = float(sys.maxsize)
            index = -1
            for i in range(len(values)):
                distTemp += self.calculate_distance(value, values[i])
            if distTemp < minDist:
                minDist = distTemp
                index = values.index(value)
            midValue = (values[index], len(values))
        yield cluster, midValue
    #Nothing special yeilds clusters
    def reduce_task(self, cluster, values):
        numbers = []
        samples = []
        for value in values:
            sample, number = value
            samples.append(sample)
            numbers.append(number)

        nc = samples[0]
        self.new_medoids.append(nc)
        yield cluster, nc

    #Define steps 
    def steps(self):
        return [self.mr(mapper=self.map_task,
                        combiner=self.combine_task,
                        reducer=self.reduce_task)]

if __name__ == '__main__':
    MRKMedoids.get_medoids()
    MRKMedoids.run()
    MRKMedoids.write_medoids()
    #print("The Percentage of Correct classifications is : %f", MRKMedoids.get_correct_percentage())