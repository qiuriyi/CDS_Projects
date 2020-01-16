from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy
from scipy import spatial

class MRMovie(MRJob):

	def steps(self):
		return [
		MRStep(mapper=self.mapper_get_names,
			reducer=self.reducer_get_names),
		MRStep(reducer=self.reducer_create_pairs),
		MRStep(reducer=self.reducer_compute_similarities),
		MRStep(reducer=self.reducer_output),
		MRStep(reducer=self.reducer_sort_results)]	

	def configure_options(self):
		super(MRMovie, self).configure_options()
		self.add_passthrough_option(
            '-l', '--lowerbound', action="store", type='float',
            default=0.4, help='Lower bound of similarity.')
		self.add_passthrough_option(
            '-p', '--minpairs', action="store", type='int',
            default=8, help='Minimum number of rating pairs.')
		self.add_passthrough_option(
            '-m', '--moviename', action="append",
            default=[], help='Movie name to look up for similar movies')
		self.add_passthrough_option(
            '-k', '--numofitems', action="store", type='int',
            default=15, help='Number of similar movies to show.')

	def mapper_get_names(self, _, line):

		if len(self.options.moviename) == 0:
			raise Exception('Must specify at least one search item.')

		mid, uid, name, rating = '0','0','0','0'
		line = unicode(line, errors='ignore')
		splits = line.strip().split('::')
		if len(splits) == 4:
			uid,mid,rating = splits[0],splits[1],splits[2]
		else:
			mid,name = splits[0],splits[1]
		yield mid, (uid,name,rating)
	

	def reducer_get_names(self, key, values):
		last_name = None
		for value in sorted(values):
			list_values = list(value)
			name, uid, rating = list_values[1], list_values[0], list_values[2]
			if uid == '0':
				last_name = name
			else:
				yield uid, (uid, last_name,rating)

	



	def reducer_create_pairs(self, _, values):
		rating_list = []
		name_list = []
		for value in values:
			list_values = list(value)
			uid = list_values[0]
			name = list_values[1]
			rating = list_values[2]
			rating_list.append(rating)
			name_list.append(name)
			if len(name_list) > 1:
				length = len(name_list)
				for i in range(length-1):
					check_abc = [name, name_list[i]]
					if sorted(check_abc)[0] == name:
						yield (name,name_list[i]), (name,name_list[i],rating,rating_list[i])
					else:
						yield (name_list[i],name), (name_list[i],name,rating_list[i],rating)

	def reducer_compute_similarities(self, _, values):
		rating_list1 = []
		rating_list2 =[]
		counter = 0
		for value in values:
			list_values = list(value)
			m1,m2,r1,r2 = list_values[0],list_values[1],list_values[2],list_values[3]
			rating_list1.append(int(r1))
			rating_list2.append(int(r2))
			movie1 = m1
			movie2 = m2
			counter += 1
		if counter >= self.options.minpairs:
			stat_cor = numpy.corrcoef(rating_list1,rating_list2)[0,1]
			cos_cor = 1 - spatial.distance.cosine(rating_list1,rating_list2)
			avg_cor = 0.5 * (stat_cor+cos_cor)
			if avg_cor > self.options.lowerbound:
				yield (movie1,movie2), (movie1,movie2,str(stat_cor),str(cos_cor),str(avg_cor),str(counter))


	def reducer_output(self, _, values):
		for value in values:
			list_values = list(value)
			m1,m2,stat_cor,cos_cor,avg_cor,counter = list_values
			for movie in self.options.moviename:
				if m1 == movie:
					yield m1,(str(1-float(avg_cor)),m1,m2,stat_cor,cos_cor,counter)
				elif m2 == movie:
					yield m2,(str(1-float(avg_cor)),m2,m1,stat_cor,cos_cor,counter)

	def reducer_sort_results(self, _, values):
		index = 0
		for value in sorted(values):
			list_values = list(value)
			avg_cor,m1,m2,stat_cor,cos_cor,counter = list_values
			if index < self.options.numofitems:
				yield m1,(m2,1-float(avg_cor),stat_cor,cos_cor,counter)
			index += 1

if __name__ == '__main__':
    MRMovie.run()
