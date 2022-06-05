import pyspark

BCpath = "BackgroundChecks.csv"
GDpath = "GunDeaths.csv"

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

stateinfo = list()
for i in range(0,50):
	stateinfo.append( list() )

def main():
	print("hello")
	df = spark.read.option("header",True).csv(BCpath)
	df2 = spark.read.option("header",True).csv(GDpath)
	
	#Get each state individually and the amount of deaths per state per year
	
	index = 0
	#For each state get all information
	for state in states:
		for i in range(2008-1999):
			year = 1999+i
			print("State:",state," YEAR:", year)
			
			#Get all info of year
			stateyear = df.filter( (df.state == state) & (df.year == year)).collect()
			stateinfo[index].append(stateyear)
		index +=1
	
	for state in stateinfo:
		for year in state:
			print(year)

if __name__ == "__main__":
    main()
