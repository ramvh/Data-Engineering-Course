from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

# import workspace.airflow.plugins.operators as operators
# import workspace.airflow.plugins.helpers as helpers
import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
