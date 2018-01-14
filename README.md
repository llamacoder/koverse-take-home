# koverse-take-home


## Koverse Take-Home Project
Tracey Dolsen
14 Jan 2018

### OVERVIEW

I wrote a transform that color-codes the records in a dataset based on the values in a single column compared against a set of ranges specified by the user.  

#### Input Parameters
In addition to the regular transform inputs, the user is prompted for the name of the field on which to color-code as well as the upper and lower boundaries of three ranges to be used for assigning colors.

#### Outputs
The transformed dataset is a copy of the original dataset with an additional field (“color”) that contains a string with a color in it based on the value in the user-specified field.

#### Uses
This transform can be used on any dataset with a numeric field.  It could be used for something as mundane as distinguishing between levels of Kickstarter project goals, or it could be used to provide mission-critical real-time color-coding for threat detection without local processing.

### PROCESS
I broke the problem down into a series of steps as described below.   

#### Add a Dataset
My first step was to find and load a dataset that I found interesting.  My first attempt failed (TED Talks data, which seems to load into a Google spreadsheet without any trouble - it is here if you want to investigate:  https://www.kaggle.com/rounakbanik/ted-talks/data).  My second attempt, Kickstarter data, worked well.  

#### Run an Existing Transform
My next step was to run a transform from the transform list on my Kickstarter dataset.  I ran the Word Count transform, since that was the one that I had example code for, and it worked well.  I tried the Java Word Count transform as well, but I did not get results.  I learned later that the transforms in the list are not guaranteed to work.

#### Build and Run Example Code
I cloned the example code Jared sent.  I spent some time learning about Maven, then I built the project and uploaded the addon.  I ran it against my Kickstarter data and got the results I expected.

#### Code and Test My New Transform
I modified the example code to perform the color-coding function that I had settled on for my solution.  I wrote and ran tests locally to make sure it was working, then I uploaded my addon.  It worked fine against a tiny dataset, but I had to add more robust error-handling to get it to work as expected against my Kickstarter dataset.  I also ran it against several of the datasets that were already loaded in the Koverse instance, and it worked as I expected.

#### Weaknesses
Because I do not have a complete understanding of the way Koverse works, I could not configure the transform to take a dynamic number of ranges as input.  I arbitrarily used three ranges, but I would want to gather actual requirements to see if the number of ranges is adequate.  Also, I hard-coded some color strings, also arbitrary, for the purposes of my example.

### REVIEW
I learned about Koverse, Maven, and Spark in order to complete this project.  I considered continuing the learning process by creating an app using Koverse’s REST API, but Jared let me know that my emphasis should be on the transform development instead and that I should “wave my hands” when I came to further development.  Were I to continue, I would have written a small app that showed the record colors for the goals field in the Kickstarter dataset.  
