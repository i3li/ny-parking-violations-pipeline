from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators

# BasicDFM = Basic Dimension Fact Modeling
class BasicDFMPlugin(AirflowPlugin):
    name = "basic_dfm"
    operators = [
        operators.StageToRedshiftOperator
    ]
