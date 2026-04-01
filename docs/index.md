# Continuous Intelligence

This site provides documentation for this project.
Use the navigation to explore module-specific materials.

## How-To Guide

Many instructions are common to all our projects.

See
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to get these projects running on your machine.

## Project Documentation Pages (docs/)

- **Home** - this documentation landing page
- **Project Instructions** - instructions specific to this module
- **Your Files** - how to copy the example and create your version
- **Glossary** - project terms and concepts

## Custom Project

### Dataset
I used a synthetic dataset that consists of different branch's profit, expenses, donations customer_wait time second informations.
For each month,
1. the monthly_profit field includes the profit the branch earned.
2. Monthly_expenses: This field includes all operational cost the branch incurs.
3. Monthly_donations: This field includes the expenses incured for community services by each branch on a monthly basis.
4.  Customer_wait_time_seconds: This field has information regarding the wait time of customers in each of the branch.

### Signals
I used calculated fields such as ,
monthly_profit_rolling_SD, monthly_expenses_rolling_SD, customer_wait_time_rolling_SD  with the existing window size of 3 . The standard deviation also helps the store to detect any unusal spikes in expenses or customer wait time and take necessary actions.


### Experiments
I added a Branch level aggregation calculation using group by  and used a left join to join this calculated fields to the original dataframe.
I added the rolling SD fields as an experiment to see how the values are calculated on a window of 3 and how the values help in identifying trends.
I built a bar chart to visualize the profit , expense and wait time to give a combination view and tell an insightful story based on the combined trends.

### Results
Based on the analysis of results, Southgate marks the highest in profit of 64,000 when compared to the other branches.
Trends for the Eastwood branch not only show a spike in wait times but also show that these stores are making a lesser profit when compared to other stores with higher rolling SD for expenses.

### Interpretation
On the whole, the retail store is making good profit, however based on individual branches, Eastwood branch needs attention so it can improve performance by adopting stratergies that reduces wait time and also stratergies to increase profit and to control expenses.

## Additional Resources

- [Suggested Datasets](https://denisecase.github.io/pro-analytics-02/reference/datasets/cintel/)
