'''
A command line tool for turning blockpy logs into ProgSnap2 format

Refer to:
    Protocol Draft: https://docs.google.com/document/d/1bZPu8LIUPOfobWsCO_9ayi5LC9_1wa1YCAYgbKGAZfA/edit#
    CodeState Representation: https://docs.google.com/document/d/1FZHBcHYAG9uC9tRdhyoPIsCrJZP_jUSNXTswDHCi-ys/edit#
    
TODO:
    I could have done more to decouple the zipfile reading from the ProgSnap2
    class, which would probably make this more reusable for others.
'''

import zipfile
import tarfile
import json
import argparse
import os
import shutil
from datetime import datetime
from collections import Counter
from pprint import pprint
from converters.progsnap2 import ProgSnap2

#try:
#    from tqdm import tqdm
#except:
#    print("TQDM is not installed")
#    tqdm = list
# from tqdm import tqdm

BLOCKPY_INSTANCE = 'BPY4'
TEMPORARY_DIRECTORY = "__temp__"

ENCODING = ProgSnap2.ENCODING

                          
class UnclassifiedEventType(Exception):
    pass

        
def blockpy_timestamp_to_iso8601(timestamp):
    '''
    Converts blockpy style timestamps into an ISO-8601 compatible timestamp.
    
    > blockpy_timestamp_to_iso8601(2018-10-31-12-02-25)
    2018-10-31T12:02:25
    
    Arguments:
        timestamp (str): A blockpy-style timestamp
    Returns:
        str: The ISO-8601 timestamp.
    '''
    return datetime.fromtimestamp(int(timestamp)).isoformat()

def add_path(structure, path, limit_depth=1):
    '''
    Given a path and a structure representing a filesystem, parses the path
    to add the components in the appropriate place of the structure.
    
    Note: This modifies the given structure!
    
    TODO: This shouldn't actually dive into student code directories. Those
    should be "flat". We should either limit the depth or just unroll the loop.
    
    Structure:
        dict[str:Structure]: A folder with nesting
        dict[str:str]: A terminal level mapping to an absolute path name.
    
    Arguments:
        structure (Structure): The representation of the filesystem.
    '''
    components = path.split("/")
    depth = 0
    while len(components) > 1:
        current = components.pop(0)
        if current not in structure:
            structure[current] = {}
        structure = structure[current]
        depth += 1
        if depth > limit_depth:
            break
    if components[0]:
        structure[components[0]] = path


def load_zipfile(input_filename, extraction_directory):
    needed_files = ['log.json']
    compressed = zipfile.ZipFile(input_filename)
    for need in needed_files:
        target = extraction_directory+"/"+need
        if os.path.exists(target.strip()):
            yield need, target
            continue
        for potential_path in ['db/'+need, need]:
            names = {zip_info.filename:zip_info for zip_info in compressed.infolist()}
            if potential_path in names:
                member = names[potential_path]
                member.filename = os.path.basename(member.filename)
                compressed.extract(need, extraction_directory)
                yield need, target
                break
        else:
            raise Exception("Could not find log.json in given file: "+input_filename)
                           
def load_tarfile(input_filename, extraction_directory):
    needed_files = ['log.json']
    compressed = tarfile.open(input_filename)
    for need in needed_files:
        target = extraction_directory+"/"+need
        # TODO: Doesn't work - why?
        if os.path.exists(target.strip()):
            yield need, target
            continue
        # Otherwise, we need to extract it
        for potential_path in ['db/'+need, need]:
            names = [tar_info.name for tar_info in compressed.getmembers()]
            if potential_path in names:
                member = compressed.getmember(potential_path)
                member.name = os.path.basename(member.name)
                compressed.extract(need, extraction_directory, set_attrs=False)
                yield need, target
                break
        else:
            raise Exception("Could not find log.json in given file: "+input_filename)
    
def make_directory(directory):
    # Remove any existing CodeStates in this directory
    if os.path.exists(directory):
        # Avoid bug on windows where a handle is sometimes kept
        dummy_dir = directory+"_old"
        os.rename(directory, dummy_dir)
        shutil.rmtree(dummy_dir)
    os.mkdir(directory)
    return directory
    
def chomp_iso_time_decimal(a_time):
    if '.' in a_time:
        return a_time[:a_time.find('.')]
    else:
        return a_time

def map_blockpy_event_to_progsnap(event, action, body):
    if event == 'code' and action == 'set':
        return {'EventType': "File.Edit", 'EditType': "GenericEdit"}
    # NOTE: We treat the feedback delivered to the student as the actual run
    #elif event == 'engine' and action == 'on_run':
    #    return 'Run.Program'
    elif event == 'editor':
        if action == 'load':
            return 'Session.Start'
        elif action == 'reset':
            return {'EventType': "File.Edit", 'EditType': "Reset"}
        elif action == 'blocks':
            return 'X-View.Blocks'
        elif action == 'text':
            return 'X-View.Text'
        elif action == 'split':
            return 'X-View.Split'
        elif action == 'instructor':
            return 'X-View.Settings'
        elif action == 'history':
            return 'X-View.History'
        elif action == 'trace':
            return 'X-View.Trace'
        elif action == 'upload':
            return 'X-File.Upload'
        elif action == 'download':
            return 'X-File.Download'
        elif action == 'changeIP':
            return 'X-Session.Move'
        elif action == 'import':
            return 'X-Dataset.Import'
        elif action in ('run', 'on_run'):
            # NOTE: Don't care about redundant news that "run" button was clicked
            return None
    elif event == 'trace_step':
        return 'X-View.Step'
    elif event == 'feedback':
        if action.lower().startswith('analyzer|'):
            return {'EventType': "Intervention",
                    'InterventionType': "Analyzer",
                    'InterventionMessage': action+"|"+body}
        
        elif action.lower() == 'editor error' or action.lower().startswith('syntax|'):
            return {'EventType': "Compile.Error",
                    'CompileMessageType': action+"|"+body}
        
        elif action.lower().startswith('complete|'):
            return {'EventType': "Run.Program",
                    'ExecutionResult': "Success",
                    'Score': 1}
        elif action.lower().startswith('runtime|') or action.lower() == 'runtime':
            return {'EventType': "Run.Program",
                    'ExecutionResult': "Error",
                    'ProgramErrorOutput': action+"|"+body}
        elif action.lower() == 'internal error':
            return {'EventType': "Run.Program",
                    'ExecutionResult': "SystemError",
                    'ProgramErrorOutput': action+"|"+body}
        
        return {'EventType': "Intervention", 'InterventionType': "Feedback",
                'InterventionMessage': action+"|"+body}
    elif event == 'engine':
        # NOTE: Don't care about the engine trigger events?
        # TODO: Luke probably cares about this, we may have to jury rig a way
        #       to attach it to the proper feedback result.
        return None
    elif event == 'instructor':
        # NOTE: Don't care about instructors editing assignments
        return None
    elif event == 'trace':
        # NOTE: Don't care about redundant activation of tracer
        return None
    elif event == 'worked_examples':
        # NOTE: Don't care about worked_examples in BlockPy
        return None
    raise UnclassifiedEventType((event, action, body))

def log_blockpy_event(progsnap, record):
    # Skip events without timestamps
    if not record['timestamp'] or record['timestamp'] == 'None':
        return (record['event'], record['action'])
    # Gather local variables
    event = record['event']
    action = record['action']
    body = record['body']
    ClientTimestamp = blockpy_timestamp_to_iso8601(record['timestamp'])
    ServerTimestamp = chomp_iso_time_decimal(record['date_created'])
    SubjectID = record['user_id']
    AssignmentID = record['assignment_id']
    # Process event types
    progsnap_event = map_blockpy_event_to_progsnap(event, action, body)
    # Wrap strings with dictionaries
    if progsnap_event == None:
        return (record['event'], record['action'])
    if isinstance(progsnap_event, str):
        progsnap_event = {'EventType': progsnap_event}
    # File edits get code states
    CodeStateID = None
    if progsnap_event['EventType'] == "File.Edit":
        CodeStateID = progsnap.log_code_state(body)
    # And actually log the event
    progsnap.log_event(ClientTimestamp=ClientTimestamp,
                       SubjectID=SubjectID,
                       AssignmentID=AssignmentID,
                       CodeStateID=CodeStateID,
                       ServerTimestamp=ServerTimestamp,
                       ToolInstances=BLOCKPY_INSTANCE,
                       **progsnap_event)
                       
    # And done
    return (event, action)

def load_blockpy_events(progsnap, input_filename, target):
    '''
    Open up a submission file downloaded from blockpy and process its events,
    putting all the events into the progsnap instance.
    
    Arguments:
        progsnap (ProgSnap2): The progsnap instance to log events to.
        submissions_filename (str): The file path to the zip file.
    '''
    filesystem = {}
    
    # Open data file appropriately
    temporary_directory = make_directory(TEMPORARY_DIRECTORY)
    if zipfile.is_zipfile(input_filename):
        data_files = load_zipfile(input_filename, temporary_directory)
    elif tarfile.is_tarfile(input_filename):
        data_files = load_tarfile(input_filename, temporary_directory)
    for name, path in data_files:
        with open(path) as data_file:
            filesystem[name] = json.load(data_file)
    pprint(filesystem['log.json'][:10])
    types = Counter()
    for event in filesystem['log.json']:
        EventType = log_blockpy_event(progsnap, event)
        types[EventType] += 1
    pprint(dict(types.items()))
    

def load_blockpy_logs(input_filename, target="exported/"):
    '''
    Load all the logs from the given files.
    
    Arguments:
        input_filename (str): The file path to the zipped file
        target (str): The directory to store all the generated files in.
    '''
    progsnap = ProgSnap2()
    load_blockpy_events(progsnap, input_filename, target)
    progsnap.export(target)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert event logs from BlockPy into the progsnap2 format.')
    parser.add_argument('input', type=str,
                        help='The dumped database zip.')
    parser.add_argument('--target', dest='target',
                        default="exported/",
                        help='The filename or directory to save this in.')
    parser.add_argument('--unzipped', dest='unzipped',
                        default=False, action='store_true',
                        help='Create an unzipped directory instead of a zipped file.')

    args = parser.parse_args()
    
    
    load_blockpy_logs(args.input, args.target)
    
