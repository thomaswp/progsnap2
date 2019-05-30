
import csv
from datetime import datetime
from converters.progsnap2 import ProgSnap2

PCRS_INSTANCES = "Python; PCRS" # TODO: Find Python and PCRS versions

if __name__ == "__main__":

    progsnap = ProgSnap2()

    path = 'C:/Users/Thomas/Documents/GitHub/SnapHints/R/PCRS/data/code-states-corrected.csv'
    with open(path) as file:
        code_states = csv.DictReader(file)
        i = 0
        for row in code_states:
            # Need to add '00' to timestamp, to match the format Python expects
            timestamp = datetime.strptime(row['timestamp'] + '00', '%Y-%m-%d %H:%M:%S.%f%z').isoformat()
            subject_id = row['user_id']
            assignment_id = row['problem_id']
            code = row['code']
            code = code[42:-42]  # Remove GUID header and footer; TODO: Make more robust (i.e. check)
            progsnap.log_event(
                EventType='Submit',
                ClientTimestamp=None,
                ServerTimestamp=timestamp,
                SubjectID=subject_id,
                AssignmentID=assignment_id,
                ToolInstances=PCRS_INSTANCES,
                CodeStateID=progsnap.log_code_state(code),
            )
            i += 1
            if i > 10: break

    progsnap.export('../test_output')
