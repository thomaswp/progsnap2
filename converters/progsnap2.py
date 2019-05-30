import csv
import os
import io
import shutil

# Some events trigger at distinct timestamps, so we arbitrarily order
# certain events over others.
# TODO: This may be BlockPy-specific
ARBITRARY_EVENT_ORDER = [
    'Submit',
    'Compile',
    'Compile.Error',
    'Program.Run',
    'Program.Test',
    'Feedback.Grade',
]
# When writing out columns, we want them in a certain order to make the
# whole thing more readable
ARBITRARY_COLUMN_ORDER = ['EventID', 'Order', 'SubjectID', 'AssignmentID',
                          'EventType', 'CodeStateID',
                          'ClientTimestamp',
                          'Score',
                          'EditType',
                          'CompileMessageType',
                          'ExecutionResult',
                          'ProgramErrorOutput',
                          'InterventionType',
                          'InterventionMessage',
                          'ServerTimestamp', 'ToolInstances']

ENCODING = 'utf8'
DUMMY_CODE_STATES_DIR = "__CodeStates__"


class ProgSnap2:
    """
    A representation of the ProgSnap2 data file being generated.

    Directory is a tuple of N files, where each element of the tuple is a
        tuple having a filename and contents paired together. This allows us
        to hash directories of files and perform deduplication.

    Attributes
        main_table (list[Event]): The current list of events.
        main_table_header (list[str]): The default headers for the table.
        csv_writer_options (dict[str:str]): Options to pass to the CSV
                                            writer, to maintain some
                                            flexibility for later.
        code_files (dict[Directory: str]): The dictionary mapping the
                                           filename/contents to the code
                                           instance IDs.
        CODE_ID (int): The auto-incrementing ID to apply to new codes.
        VERSION (int): The current Progsnap Standard Version
    """


    VERSION = 3
    
    def __init__(self, csv_writer_options=None):
        if csv_writer_options is None:
            csv_writer_options = {'delimiter': ',', 'quotechar': '"',
                                  'quoting': csv.QUOTE_MINIMAL}
        self.csv_writer_options = csv_writer_options
        # Actual data contents
        self.main_table_header = list(ARBITRARY_COLUMN_ORDER)
        self.main_table = []

        self.code_files = {'': 0}  # {tuple(): 0} # TODO: Should the first code state always be empty?
        self.CODE_ID = 1

    def export(self, directory):
        """
        Create a concrete, on-disk representation of this event database.

        Arguments:
            directory (str): The location to store the generated files.
        """
        self.export_metadata(directory)
        self.export_main_table(directory)
        self.export_code_states(directory)

    def export_metadata(self, directory):
        """
        Create the metadata table, which is more or less a constant file.

        Arguments:
            directory (str): The location to store the generated files.
        """
        # TODO: Make configurable
        metadata_filename = os.path.join(directory, "DatasetMetadata.csv")
        with _make_file(metadata_filename) as metadata_file:
            metadata_writer = csv.writer(metadata_file,
                                         **self.csv_writer_options)
            metadata_writer.writerow(['Property', 'Value'])
            metadata_writer.writerow(['Version', self.VERSION])
            metadata_writer.writerow(['AreEventsOrdered', 'true'])
            metadata_writer.writerow(['IsEventOrderingConsistent', 'true'])
            metadata_writer.writerow(['CodeStateRepresentation', 'Directory'])

    def export_main_table(self, directory):
        """
        Create the main table file.

        Arguments:
            directory (str): The location to store the generated files.
        """
        main_table_filename = os.path.join(directory, "MainTable.csv")
        with _make_file(main_table_filename) as main_table_file:
            main_table_writer = csv.writer(main_table_file,
                                           **self.csv_writer_options)
            self.finalize_table()
            optionals = Event.distill_parameters(self.main_table)
            header = self.main_table_header
            header.sort(key=Event.get_parameter_order)
            main_table_writer.writerow(header)
            for row in self.main_table:
                finalized_row = row.finalize(optionals)
                main_table_writer.writerow(finalized_row)

    @staticmethod
    def _new_code_states_directory(directory):
        """
        Creates the CodeStates directory in the given `directory`. If the
        CodeStates folder is already there, it wipes it (using a trick) to
        prevent windows from fussing.

        Args:
            directory (str): The location to make the new CodeStates directory.
        """
        code_states_dir = os.path.join(directory, "CodeStates")
        # Remove any existing CodeStates in this directory
        if os.path.exists(code_states_dir):
            # Avoid bug on windows where a handle is sometimes kept
            dummy_dir = os.path.join(directory, DUMMY_CODE_STATES_DIR)
            os.rename(code_states_dir, dummy_dir)
            shutil.rmtree(dummy_dir)
        os.mkdir(code_states_dir)
        return code_states_dir

    def export_code_states(self, directory):
        """
        Create the CodeStates directory and all of the code state files,
        organized by their unique ID.

        Arguments:
            directory (str): The location to store the generated files.
        """
        # TODO: Add options for table (and git) format
        code_states_dir = self._new_code_states_directory(directory)
        for files, CodeStateID in self.code_files.items():
            code_state_dir = os.path.join(code_states_dir, str(CodeStateID))
            if not os.path.exists(code_state_dir):
                os.mkdir(code_state_dir)
            if isinstance(files, str):
                code_state_filename = os.path.join(code_state_dir, '__main__.py')
                with _make_file(code_state_filename) as code_state_file:
                    code_state_file.write(files)
            else:
                for filename, contents in files:
                    code_state_filename = os.path.join(code_state_dir, filename)
                    with _make_file(code_state_filename) as code_state_file:
                        code_state_file.write(contents)

    def finalize_table(self):
        """
        Sort the timestamps of the events.
        Add in order (and CodeStateID if it's missing)
        """
        self.main_table.sort(key= Event.get_order)
        # Go fetch first code_states for everything
        first_code_states = {}
        for event in self.main_table:
            identifier = (event.SubjectID, event.AssignmentID)
            if identifier in first_code_states:
                continue
            current_code_state = first_code_states.get(identifier, 0)
            if event.CodeStateID is not None:
                first_code_states[identifier] = event.CodeStateID
        # Fix order attribute, make sure CodeStateID is correct
        order = 0
        CodeStateID = 0
        code_states = {}
        score_state = {}
        for event in self.main_table:
            identifier = (event.SubjectID, event.AssignmentID)
            if identifier not in first_code_states:
                first_code_states[identifier] = 0
            current_code_state = code_states.get(identifier, first_code_states[identifier])
            current_score_state = score_state.get(identifier, 0)
            if event.Score is None:
                event.Score = current_score_state
            else:
                current_score_state = event.Score
            if event.CodeStateID is None:
                event.set_ordering(order, current_code_state)
            else:
                current_code_state = event.CodeStateID
                event.set_ordering(order)
            order += 1
            code_states[identifier] = current_code_state
            score_state[identifier] = current_score_state

    def log_event(self, **kwargs):
        """
        Add in a new event to the ProgSnap2 instance.

        Arguments:
            when (str): the timestamp to use when ordering these events.
                        Currently using the ClientTimestamp.
            SubjectID (str): Uniquely identifying user id.
            EventID (str): An EventType, such as the ones documented for
                            the standard.
            kwargs (dict[str:Any]): Any optional columns for this row; the
                                    keys must match to actual columns in
                                    the progsnap standard (e.g., ParentEventID)
        Returns:
            Event: The newly created event
        """
        # TODO: events have required parameters... which should be required (and maybe from enums?)
        new_event = Event(**kwargs)
        self.main_table.append(new_event)
        return new_event

    def log_code_state(self, submission, zipped):
        """
        Add in a Submit event, which has associated code in the zip file.

        Arguments:
            submission (str or dict[str:str]): A dictionary that maps
                                               local filenames to their
                                               absolute path in the zip
                                               file. Alternatively, the raw
                                               string of the code.
            zipped (ZipFile): A zipfile that has the students' code in it.
        Returns:
            Event: The newly created event
        """
        if isinstance(submission, str):
            code = submission
        else:
            code = []
            for filepath, full in submission.items():
                contents = load_file_contents(zipped, full)
                code.append((filepath, contents))
            code = tuple(sorted(code))
        return self.hash_code_directory(code)

    def hash_code_directory(self, code):
        """
        Take in a tuple of tuple of code files and hash them into unique IDs,
        returning the ID of this particularly given code file.
        Note: Currently hashing just based on order received - possibly need
        something more sophisticated?

        Arguments:
            code (tuple of tuple of str): A series of filename/contents paired
                                          into a tuple of tuples, sorted by
                                          filenames.
        Returns:
            int: A unique ID of the given code files.
        """
        if code in self.code_files:
            CodeStateID = self.code_files[code]
        else:
            CodeStateID = self.CODE_ID
            self.code_files[code] = self.CODE_ID
            self.CODE_ID += 1
        return CodeStateID


class Event:
    """
    Representation of a given event.

    Attributes:
        EventType (str): Taken from parameter
        EventID (int): Assigned from an auto-incrementing counter
        Order (int): Assigned after all the events are created.
        SubjectID (str): Taken from parameter
        tool_instances (str): Taken from global constant
        CodeStateID (int): The current code state for this event.
        ServerTimestamp (str): Taken from parameter
        EVENT_ID (int): Unique, auto-incrementing ID for the events
    """
    MAX_EVENT_ID = 0

    def __init__(self, ClientTimestamp, SubjectID, EventType, AssignmentID,
                 ServerTimestamp, ToolInstances, Score=None, CodeStateID=None, **kwargs):
        # TODO: PS2 should enforce the correct timestamp format
        self.ClientTimestamp = ClientTimestamp
        self.ServerTimestamp = ServerTimestamp
        self.SubjectID = SubjectID
        self.AssignmentID = AssignmentID
        self.EventType = EventType
        # TODO: CodeStateID should be auto-hashed by this class
        self.CodeStateID = CodeStateID
        self.Score = Score
        self.ToolInstances = ToolInstances
        self._optional_parameters = kwargs
        # Keep track of events
        self.EventID = self._track_new_event()
        # Private fields not related to dataset
        # TODO: order should also be auto-generated if desired
        self.Order = None

    @classmethod
    def _track_new_event(cls):
        new_event_id = cls.MAX_EVENT_ID
        cls.MAX_EVENT_ID += 1
        return new_event_id

    def set_ordering(self, Order, CodeStateID=None):
        """
        This method is meant to update the relative attributes after all the
        events have been processed and ordered appropriately.

        Arguments:
            Order (int): The new order for this event
            CodeStateID (int|None): The new code state for this event.
        """
        self.Order = Order
        if CodeStateID is not None:
            self.CodeStateID = CodeStateID

    def finalize(self, default_parameter_values):
        """
        Fill in any missing optional parameters for this row, sort the all
        parameters into the right order.

        Arguments:
            default_parameter_values (dict[str: Any]): A dictionary of the
                                                       default values for
                                                       all of the optional
                                                       parameters.
        """
        # Avoid mutating original
        parameter_values = dict(default_parameter_values)
        parameter_values.update(self._optional_parameters)
        required_columns = {COLUMN: getattr(self, COLUMN)
                            for COLUMN in ARBITRARY_COLUMN_ORDER
                            if hasattr(self, COLUMN)}
        parameter_values.update(required_columns)
        sorted_parameters = sorted(parameter_values.items(),
                                   key=lambda i: Event.get_parameter_order(i[0]))
        ordered_values = [value for parameter, value in sorted_parameters]
        return ordered_values

    @staticmethod
    def distill_parameters(events):
        """
        Given a set of events, finds all of the optional parameters by
        unioning the parameters of all the events.

        Arguments:
            events (list[Event]): The events to distill all the parameters
                                    from.
        Returns:
            dict[str:str]: The mapping of parameters to empty strings.
                           TODO: The plan was to have default values, but
                                 that seems unnecessary now. Maybe should
                                 just be a set instead?
        """
        optional_parameters = set()
        for event in events:
            optional_parameters.update(event._optional_parameters)
        return {p: "" for p in optional_parameters}

    def get_order(self):
        """
        Create a value representing the absolute position of a given
        event. Useful as a key function for a sorting.

        Returns:
            str: The timestamp
        """
        return self.ClientTimestamp

    @staticmethod
    def get_parameter_order(parameter):
        """
        Identifies what order this parameter should go in. Useful as a key
        function for sorting. It uses the ARBITRARY_COLUMN_ORDER, but if
        the number isn't found, then the sorting will rely on
        alphabetical ordering of the parameters.

        Arguments:
            parameter (str): A column name for a ProgSnap file.

        Returns:
            tuple[int,str]: A pair of the arbitrary column order and the
                            parameter's value, allowing you to break ties with
                            the latter.
        """

        if parameter in ARBITRARY_COLUMN_ORDER:
            # print(ARBITRARY_COLUMN_ORDER.index(parameter))
            return ARBITRARY_COLUMN_ORDER.index(parameter), parameter
        return len(ARBITRARY_COLUMN_ORDER), parameter


def _make_file(filename):
    return open(filename, 'w', newline='', encoding=ENCODING)


def load_file_contents(zipped, path):
    """
    Reads the contents of the zipfile, respecting Unicode encoding... I think.

    Arguments:
        zipped (ZipFile): A zipfile to read from.
        path (str): The path to the file in the zipfile.

    Returns:
        str: The contents of the file.
    """
    data_file = zipped.open(path, 'r')
    data_file = io.TextIOWrapper(data_file, encoding=ENCODING)
    return data_file.read()