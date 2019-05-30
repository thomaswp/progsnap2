
import csv
from datetime import datetime
from converters.progsnap2 import ProgSnap2

if __name__ == "__main__":

    progsnap = ProgSnap2()

    path = 'C:/Users/Thomas/Documents/GitHub/SnapHints/R/PCRS/data/code-states-corrected'
    with open(path) as file:
        code_states = csv.DictReader(file)
        for row in code_states:
            timestamp = datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S%z').isoformat() # e.g. 2017-09-21 17:07:40.499051-04
            subject_id = row['user_id']
            assignment_id = row['problem_id']
            progsnap.log_event(
                EventType='Submit',
                ServerTimestamp=timestamp,
                SubjectID=subject_id,
                AssignmentID=assignment_id,

            )
