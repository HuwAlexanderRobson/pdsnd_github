import allpandas as pd
import os

#set the folder containingall my files as the active directory
#os.chdir(r"C:\Users\huwro\Udacity_Projects\Udacity Pgm for DS Proj 2\bikeshare-2")
# fishy



"""-----------------------------------------------------------------------------------------------------------------
Section 1 - two functions that take the input passed by the user and produce the output statistics. 

The first extracts and filters the data set to be consistent with the input parameters passed by the user
The second calculates the requested statistics from that filtered data set

"""


def load_data2(city,month,day):
    
    """
    Loads data for the specified city and filters by month and day if applicable.

    Args:
        (str) city - name of the city to analyze. String for an individual city, a list if you want multiple cities or "all" to see all cities in the dataset.
        (str) month - name of the month to filter by, or a list of months, or "all" to see statistics for all months values in the data set.
        (str) day - name of the day of week to filter by, or a list of days, or "all" to see statistics for all day values in the data set. 
    Returns:
        df - Pandas DataFrame containing data filtered by month, day and city.
        
    """
    

    # load the data for the relevant cities

    # filter by city where necessary
    if city!=["all"]:
        
        # check if New York is in the list of cities provided to resolve the "_" character issue
        
        city=pd.Series(city)
        
        #get a series of the indicies which relate to new York in the city input list
        #using the series method, not the string method to replace whole entries in the series, not substrings of entries
        repl_index=city[city.str.contains("[Nn]ew [Yy]ork[^*]",regex=True)].index
    
        #replace the strings in city for the idenfied indecies
        city[repl_index]="new_york_city"
        
        #city=city.replace(to_replace="[Nn]ew [Yy]ork[^*]", value="new_york_city",regex=True)
        #city = city.map({})
    
        #create the dict that identifies the relevant subset of the overall df based on cities
        cities_list = []


        # now read the csv file from the dir for the cities in the selected dict
        for c in city:
            filepath = "input_data_files/"+c+".csv"
            cities_list.append(pd.read_csv(filepath))
        
        #make a single df from the list of dfs with concat function.
        # axis = 0 says that concatenate over rows (vertically)
        df=pd.concat(cities_list, axis= 0, ignore_index=False, keys=city, names=['cities','row index'], sort=True)

        #create a catagorical variable in the series with the city values against the lines
        df2 = pd.Series(df.index.get_level_values(0))
        
        df=pd.merge(df,df2, how='inner', on=df.index, sort=True)
        
        # delete the index of the previous df and "Unamed: 0", which are unecessary
        df.drop(columns=["key_0","Unnamed: 0"],inplace=True)
    
    else:

        #loop through all of the .csv files in "input_data_files"
        filepath = "input_data_files/"
        file_list = os.listdir(filepath)
        cities_list =[]

        # now read the csv file from the dir for all of the cities for which there are data files
        for f in file_list:
            cities_list.append(pd.read_csv("input_data_files/"+f))

        # extract a list of all of the city names in the input data
        city = [f.split(".")[0].capitalize() for f in file_list]
 
        #make a single df from the list of dfs with concat function.
        # axis = 0 says that concatenate over rows (vertically)
        df=pd.concat(cities_list, axis= 0, ignore_index=False, keys=city, names=['cities','row index'], sort=True)

        #create a catagorical variable in the series with the city values against the lines
        df2 = pd.Series(df.index.get_level_values(0))
        
        df=pd.merge(df,df2, how='inner', on=df.index, sort=True)

        # delete the index of the previous df and "Unamed: 0", which are unecessary
        df.drop(columns=["key_0","Unnamed: 0"],inplace=True)
 

  
    #convert 'Start Time' from object/str dtype to data time
    df['Start Time']=pd.to_datetime(df['Start Time'])

    # Extract the month and day components and make new cols in the df using the datetime (denoted dt) class of methods and properties and the month and day properties
    
    #note dt object class month property makes 1-12 integers not names
    df['Month']=df['Start Time'].dt.month
    df['Day']=df['Start Time'].dt.weekday_name

    #filter the data for the relevant months and days where necessary

    # month filter

    if month!=["All"]:

        # if supplied one month convert to a list
        
        if type(month) is not list:
            month_list = []
            month_list.append(month)
            month=month_list

        # convert mnth names to integers    
        month_names=pd.Series(['January','February','March','April','May','June','July','August','September','October','November','December'])
    
        # make a list of the repsective integers assoicated with the month names
        month=list(month_names[month_names.isin(month)].index)
        
        # filter df by the relevant months

        df=df.loc[df['Month'].isin(month)]

        # now convert the output df's month values to the month names
        
        # first make the dict to go into mapping function
        zip_iterator = zip([i for i in range(1,13)],month_names) #get a list of tuples
        mnth_mapping_dict = dict(zip_iterator)
        
        #do the mapping
        df['Month']=df['Month'].map(mnth_mapping_dict)

    else:
        # get a list of mnth names to convert to integers
        month_names=pd.Series(['January','February','March','April','May','June','July','August','September','October','November','December'])

        # now convert the output df's month values to the month names
        
        # first make the dict to go into mapping function
        zip_iterator = zip([i for i in range(1,13)],month_names) #get a list of tuples
        mnth_mapping_dict = dict(zip_iterator)

        #do the mapping
        df['Month']=df['Month'].map(mnth_mapping_dict)


    # day filter

    if day !=["All"]:

        if type(day) is not list:
            day = [day]

        df=df.loc[df['Day'].isin(day)]


    #finally make a column of combinations of start and end stations
    df['Trip']=pd.Series(zip(df['Start Station'],df['End Station'])).apply(lambda x: "From "+ x[0]+ " to "+x[1])

    #note one can also do it as with basic python string operator:
    #df['Trip']=df['Start Station']+df['End Station']


    #we reset the index of the df - note that if you filter out certain rows of a df the original index remains
    # if you then want to use indexing on rows, .e.g. show me the first five rows, you will see only rows in the filtered df that had an index of 5 or less in the orginal dataframe
    df.reset_index(inplace=True)

    # the old index is preserved as a column, thus we must remove it
    df.drop(columns="index",inplace=True)

    # UX - clean up the output df 

    #for user experience, replace 'new_york_city' with 'New York City' and rename columns
    df.rename(columns={"cities":"City"}, inplace=True)
    df['City'].replace(to_replace={"new_york_city": "New York City","washington":"Washington","chicago":"Chicago"}, inplace=True)

    return df



def calc_stats(df):

    """
    Calculates the output statistics using the dataframe generated by function load_data2 as its domain.

    Args:
       dataframe output of load_data2 function
    Returns:
       dictionary of stats split by city, user type and gender.
        
    """
    
    import statistics as st
    
    #1 Popular times of travel 
    # note .droplevel() is used to remove the unecessary hierarchical index

    travel_time_stats=df.groupby(by=df['City'],as_index=True)[['Month','Day']].apply(pd.DataFrame.mode).droplevel(level=1)

    #2 Popular stations and trip
    location_stats=df.groupby(by=df['City'],as_index=True)[['Start Station','End Station','Trip']].apply(pd.DataFrame.mode).droplevel(level=1)
    
    #3 Trip duration
    duration_stats_sum = df.groupby(by=df['City'],as_index=True)[['Trip Duration']].sum().rename(columns={'Trip Duration': 'Total Trip Duration (Mins)'})
    duration_stats_avg = df.groupby(by=df['City'],as_index=True)[['Trip Duration']].mean().rename(columns={'Trip Duration': 'Average Trip Duration (Mins)'})

    duration_stats = pd.merge(duration_stats_sum, duration_stats_avg, on=duration_stats_avg.index).rename(columns={"key_0": "City"})
                               
    #4 User characteristics 
    user_characteristic_stats = df.groupby(['City','User Type','Gender'])[['User Type']].count().rename(columns={'User Type':'User Count'})

    #5 User birth yrs
    earliest_birth_yr = df.groupby(by=df['City'],as_index=True)['Birth Year'].min()
    latest_birth_yr = df.groupby(by=df['City'],as_index=True)['Birth Year'].max()
    most_common_birth_yr =df.groupby(by=df['City'],as_index=True)['Birth Year'].apply(pd.Series.mode).droplevel(level=1)
    
    # concat the three dfs with a common structure into one over column axis
    user_birth_stats=pd.concat([earliest_birth_yr, latest_birth_yr, most_common_birth_yr], axis=1,sort=True)
    #reset column values of the new df
    user_birth_stats.columns=["Earliest Birth Year","Latest Birth Year","Most Common Birth Year"]

    bike_stats = {"travel time stats": travel_time_stats, "travel location stats": location_stats, "travel duration stats":duration_stats, "user characteristic stats": user_characteristic_stats,"user birth stats": user_birth_stats}
 
    return bike_stats




"""------------------------------------------------------------------------------------------------------------------------------
Section 2 
User input and output provided generated using function calls on the functions listed above.
"""


"""
Section 2.1. 
Get the input from the user in the necessary format to generate the ouptut.

"""

print("Please provide values for the main dimensions when prompted. These will filter the data used to calculate the stats.")
print("You can write one value or multiple values seperated by a comma.")

# define a bool variable whose value is set to prompt the user to reenter if they did not provide input values of the correct type


def user_input(i):

    """
    This function handles the user inputs for the main dimensions. 
    
    It takes a string inputted by the user and returns a list of distinct dimension values that the above function load_data2() takes as the dimension values to filter the raw data on. 
    
    The user must input a string. If they want to filter on multiple values for a dimension they must seperate each of the values with a comma in the string. 
    
    """

    if i=="city":
        while True: 
            m = input("Which cities are you interested in? : ")
            m=[s.strip().lower() for s in m.split(",")]
            # verify correct data type entered
            if not all([e in ["chicago","washington", "new york city"] for e in m]) and m[0]!="all":
                print("Those cities aren't valid unfortunately. Pls try again.")
            else:
                break
    elif i == "mnth":
        while True: 
            m = input("What months are you interested in? :  ")
            # turn the input into the format that the function load_data2 accepts when filtering on month
            # First, strip all of the spaces left around the commas the user inputs
            #Then, convert all to lower case to remove any captialised characters not at the start of the string
            #Finally,  captialise the strings in m so that first character is a capital
            m=[s.strip().lower().capitalize() for s in m.split(",")]
            # verify correct data type entered
            if not all([e in ['January','February','March','April','May','June','July','August','September','October','November','December'] for e in m]) and m[0]!="All":
                print("Those months aren't valid unfortunately. Pls try again.")   
            else: 
                break                              
    elif i == "day":
        while True:
            m = input("What days are you interested in? :  ")
            # turn the input into the format that the function load_data2 accepts when filtering on day
            # First,  strip all of the spaces left around the commas the user inputs
            #Then, convert all to lower case to remove any captialised characters not at the start of the string
            # Finally, captialise the strings in m so that first character is a capital
            m=[s.strip().lower().capitalize() for s in m.split(",")]
            # verify correct data type entered
            if not all([e in ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'] for e in m]) and m[0]!="All":
                print("Those days aren't valid unfortunately. Pls try again.")   
            else: 
                 break
    elif i == "raw":
        while True: 
            m = input("Do you want to see the raw data? Pls type yes or no:  ").lower()
            # verify correct data type entered
            if m.lower() not in ['yes','no']:
                print("That isn't valid. Pls try again.")
            else:
                break

    return m


# Obtain the cities
print("Pls choose the cities you want stats for from Chicago, Washington or New York City. Pls input a city ")
x = user_input("city")

# Obtain the months
print("Pls choose the months you want stats for. Pls input a month as a capitalised string or list of strings")
y = user_input("mnth")

# Obtain the days
print("Pls choose the days you want stats for. Pls input a day as a capitalised string or list of strings")
z = user_input("day")

# Determine if the user wants raw data
r = user_input("raw")


"""
Section 2.2.
Calling the functions defined in section 1 on the input variables entered by the user in section 2.1.
"""

def make_output(x,y,z):

    """
    This function calls first the function load_data2 to produce a dataframe containing the bikeshare input data filtered to include only data points for the cities, days and months specified by the user.
    It then calls the function calc_stats which takes the dataframe output of load_data2 to produce the output of descriptive statistics.

    Args: exactly the same as those specified in docustring for function load_data_2.
    Output: bike_stats dataframe of output stats presented as a table and the dataframe of filtered base data, stats_input_df.    
    """
    stats_input_df = load_data2(x,y,z)

    bike_stats = calc_stats(stats_input_df)

    return bike_stats, stats_input_df


output_dict, raw_data = make_output(x,y,z)

check_type = True

while check_type == True:
    
    print("Which stats do you want to see: Travel Time Stats,Travel Location Stats,Travel Duration Stats, User Characteristic Stats, User Birth Stats")
    input_str =input("Please enter one of these strings, or multiple strings seperated by a comma:   ")
    print("\n ---------------------------------------------------------------------- \n")

    # if there are mutliple cities in the input string we now need to split to form a list
    input_list = [s.strip().lower() for s in input_str.split(",")]

    # verify correct data type entered
    if not all([e in ['travel time stats','travel location stats','travel duration stats', 'user characteristic stats', 'user birth stats'] for e in input_list]):
        print("There was an error in the strings that you entered. Pls try again, writing string names exactly as prompted.")   
    else: 
        for i in input_list:
            print(output_dict[i])
            print("\n ---------------------------------------------------------------------- \n")
        break


if r == "yes":

    row = 0
    print(raw_data.loc[row:row+4])
    print("\n ---------------------------------------------------------------------- \n")
    
    n = input("Do you want to see another 5 rows of data? Pls type yes or no:  ")

    while n == 'yes':
        row=+5
        print(raw_data.loc[row:row+4])
        print("\n ---------------------------------------------------------------------- \n")
        n = input("Do you want to see another 5 rows of data? Pls type yes or no:  ")
      


