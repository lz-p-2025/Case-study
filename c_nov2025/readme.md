
Please review the documentation for the Github API. We want to be able to process and
analyze pull requests information for different Github repositories. Your goal is to
prepare a data pipeline that will read the pull requests for the provided repositories and
load them into Synapse for a further analysis. You can pick any database or any other
output format of your choice as well (in this case, please describe the advantages of
your choice and make sure it is convenient for the downstream processing and
analysis).


Tech Details and Requirements
- In order to build the pipeline in Azure Cloud environment, create a free Azure
project to complete this case study(if this is not possible please reach out to us
so we can create a sandbox project for you).
> I have timeboxed this python task to three hours of my personal time. I have not
setup an Azure Cloud environment and not used Synapse as it is unclear the value of 
setting this up for the sake of an interview task. I have included a question around 
Synapse in my case study slide deck.

- Prepare a pipeline implemented with Python programming language to read the
pull-requests for the provided Github repositories every day and load them into
the Synapse table.
> The pipeline is implemented in `pipeline.py` and I have added detailed comments.
To run the pipeline see `run.sh`. This pipeline is run locally and
if in an environment where it could run such as on a databricks job everyday it would be 
on a cron schedule such as: "0 0 3 * * ?" to run at 3am everyday.

- Load historical data for the last 30 days if it is possible.
> See `pipeline.py`.

- Remember to clean up the data if necessary.
> See `pipeline.py`.

- There should be information on how we can make it run (what do we need to
install? How do we execute the pipeline?).
> See `run.sh`

- The code should be well documented.
> See each additional `readme.md` file for further details as well as `pipeline.yml`
and `deployment.yml`

- How would you test and deploy further changes in the code?
> For further changes I would continue using unit tests for small functionality, and also
testing in the deployment environment on a compute resource. Such as if using databricks, 
deploying a job in a dev environment and testing all aspects of the deployment, running of 
the job, and any cluster configurations. I would use an iterative approach to implement changes
which would be alongside how the product in general evolves. 

- What would be the best way to monitor the performance of the pipeline?
> Some kind of monitoring tool that can show the performance of the clusters where the
pipelines are running to see if the CPU is being utilised, or indications if the clusters
need to be sized up or down. There is a new feature of Databricks that can create
this from the information_schema. An alert to be attached to this depending on the business
use case, such as if a pipeline fails then sending an alert on teams to a channel for a team
to triage. Also seeing if the pipelines need to retry or failing often and the reasons
behind it. For example if the pipeline is retrying because of too many API requests, then
finding a solution to spread out the API calls to reduce this rate limit.


