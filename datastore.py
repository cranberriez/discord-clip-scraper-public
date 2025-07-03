from google.cloud import firestore
import random, logging
from datetime import datetime, timedelta


class DatastoreHandler:
    def __init__(self, log_item):
        self.db = firestore.Client()  # Firestore client instance
        self.log_item = log_item
        self.message_kind = "messages"  # Collection for messages
        self.userdata_kind = "userdata"  # Collection for user data


    # -------------------------
    # Helper Methods
    # -------------------------
    def _get_document_ref(self, collection, doc_id):
        """Retrieve a Firestore document reference."""
        return self.db.collection(collection).document(doc_id)


    def _log_error(self, error_message, e=None):
        """Log errors with additional context."""
        if e:
            self.log_item(f"{error_message}: {e}", logging.ERROR)
        else:
            self.log_item(error_message, logging.ERROR)

    # -------------------------
    # Message Operations
    # -------------------------
    def get_all_messages(self):
        """Retrieve all messages from the datastore."""
        self.log_item("Fetching all messages from the datastore.")
        try:
            docs = self.db.collection(self.message_kind).stream()
            messages = []
            for doc in docs:
                data = doc.to_dict()
                messages.append(data)
            self.log_item(f"Retrieved {len(messages)} messages from the datastore.", logging.INFO)
            return messages
        except Exception as e:
            self._log_error("Error fetching all messages", e)
            return []


    def get_all_msg_ids(self):
        """Get all message IDs in the messages collection."""
        self.log_item("Getting all message IDs")
        try:
            docs = self.db.collection(self.message_kind).stream()
            return [doc.id for doc in docs]
        except Exception as e:
            self._log_error("Error retrieving message IDs", e)
            return []


    def get_msg_by_id(self, msg_id):
        """Retrieve a specific message by its ID."""
        if not msg_id:
            self._log_error("Message ID is required to retrieve a message.")
            return None

        self.log_item(f"Getting message by ID {msg_id}")
        try:
            doc_ref = self._get_document_ref(self.message_kind, msg_id)
            doc = doc_ref.get()
            if doc.exists:
                return doc.to_dict()
            self.log_item(f"Message ID {msg_id} not found.")
            return None
        except Exception as e:
            self._log_error(f"Error retrieving message ID {msg_id}", e)
            return None


    def push_msg(self, message):
        """Update or replace a single message in Firestore."""
        msg_id = message.get("Id")
        if not msg_id:
            self._log_error("Message must have an 'Id' to be pushed.")
            return

        self.log_item(f"Pushing message ID {msg_id}")
        try:
            doc_ref = self._get_document_ref(self.message_kind, msg_id)
            doc_ref.set(message)  # Update or replace the document
            self.log_item(f"Message ID {msg_id} successfully pushed.")
        except Exception as e:
            self._log_error(f"Error pushing message ID {msg_id}", e)


    def push_batch_msgs(self, messages):
        """Update or replace multiple messages in Firestore."""
        if not messages:
            self._log_error("No messages to push in batch.")
            return

        self.log_item(f"Pushing {len(messages)} messages in batch.")
        batch = self.db.batch()
        try:
            for message in messages:
                msg_id = message.get("Id")
                if not msg_id:
                    self.log_item("Skipping message without 'Id'.")
                    continue
                doc_ref = self._get_document_ref(self.message_kind, msg_id)
                batch.set(doc_ref, message)  # Update or replace the document
            batch.commit()
            self.log_item(f"Successfully pushed {len(messages)} messages.")
        except Exception as e:
            self._log_error("Error pushing batch messages", e)

    # -------------------------
    # Userdata Operations
    # -------------------------
    def push_user_data(self, userdata={}):
        self.log_item("unsetup fucntion call", logging.ERROR)
        pass

    def push_batch_user_data(self, userdata={}):
        if not userdata:
            self._log_error("No userdata to push in batch.")
            return

        self.log_item(f"Pushing {len(userdata)} userdata in batch.")
        batch = self.db.batch()
        try:
            for username, user_details in userdata.items():
                if not username:
                    self.log_item("Skipping user without a username.")
                    continue
                
                # Ensure user details contain the necessary fields
                if not user_details.get("Name"):  # removed to handle deleted users # or not user_details.get("Url")
                    self.log_item(f"Skipping invalid user details for {username}.")
                    continue
                
                # Get the document reference using the username as the key
                doc_ref = self._get_document_ref(self.userdata_kind, username)
                
                # Set the document with the user details
                batch.set(doc_ref, user_details)

            batch.commit()
            self.log_item(f"Successfully pushed {len(userdata)} users.")

        except Exception as e:
            self._log_error("Error pushing batch user data", e)

    # -------------------------
    # Runtime Operations
    # -------------------------
    def get_all_runtimes(self):
        """Fetch all runtimes and return in object of key=uuid(hash), value=runtime(float)"""
        try:
            # Query all documents from the datastore kind
            query = self.db.collection(self.message_kind).stream()

            # Create a dictionary of runtimes
            runtimes = {}
            for doc in query:
                data = doc.to_dict()
                key = doc.id  # The document ID acts as the key
                runtime = data.get("Runtime")  # Fetch the 'Runtime' field

                # Only add to the dictionary if runtime exists and is not None
                if runtime is not None:
                    runtimes[key] = runtime

            return runtimes
        except Exception as e:
            self._log_error("Error fetching all runtimes", e)
            return {}


    def get_runtime_for_msg_id(self, msg_id):
        """Fetch the runtime for a single message"""        
        if not msg_id:
            return None
        
        self.log_item(f"Getting runtime for message {msg_id}")
        try:
            doc_ref = self._get_document_ref(self.message_kind, msg_id)
            doc = doc_ref.get()
            if doc.exists:
                # Fetch the 'Runtime' field specifically
                runtime = doc.get("Runtime")
                return runtime if runtime is not None else None
            self.log_item(f"Message ID {msg_id} not found.")
            return None
        except Exception as e:
            self._log_error(f"Error retrieving message ID {msg_id}", e)
            return None


    def add_runtime_to_message(self, runtime):
        """Add or update the 'Runtime' key in the corresponding message."""
        if not runtime or len(runtime) != 2:
            self._log_error("Runtime must be a tuple of (msgId, runtime_value).")
            return

        msg_id, runtime_value = runtime
        if not msg_id:
            self._log_error("Message ID is required to add runtime.")
            return

        self.log_item(f"Adding runtime to message ID {msg_id}")
        try:
            doc_ref = self._get_document_ref(self.message_kind, msg_id)
            doc = doc_ref.get()

            if not doc.exists:
                self.log_item(f"Message ID {msg_id} not found.")
                return

            # Update the document with the new runtime
            doc_ref.update({"Runtime": runtime_value})
            self.log_item(f"Runtime {runtime_value} successfully added to message ID {msg_id}.")
        except Exception as e:
            self._log_error(f"Error adding runtime to message ID {msg_id}", e)


    def push_batch_runtimes(self, runtimes):
        """Add runtimes to their corresponding messages in batch."""
        if not runtimes:
            self._log_error("No runtimes to push in batch.")
            return

        self.log_item(f"Pushing {len(runtimes)} runtimes in batch.")
        batch = self.db.batch()
        try:
            for runtime in runtimes:
                # Unpack runtime obj
                msg_id = runtime.get("Id")
                runtime_value = runtime.get("Length")
                if not msg_id or runtime_value is None:
                    self.log_item(f"Skipping runtime with missing msgId or runtime value")
                    continue

                # Fetch the document reference
                doc_ref = self._get_document_ref(self.message_kind, msg_id)
                doc = doc_ref.get()

                # Ensure the document exists
                if not doc.exists:
                    self.log_item(f"Message ID {msg_id} not found. Skipping.")
                    continue

                # Update the document with the new runtime
                batch.update(doc_ref, {"Runtime": runtime_value})

            batch.commit()
            self.log_item(f"Successfully pushed runtimes for {len(runtimes)} messages.")
        except Exception as e:
            self._log_error("Error pushing batch runtimes", e)


def generate_sample_message(msg_id):
    """Generate a sample message dictionary."""
    return {
        "Id": msg_id,
        "Discord_id": str(random.randint(10000, 99999)),
        "Poster": f"User{random.randint(1, 100)}",
        "Date": datetime.now().isoformat(),
        "Link_to_message": f"http://example.com/messages/{msg_id}",
        "Description": "This is a test message. Updated!",
        "Attachment_URL": f"http://example.com/images/{msg_id}.png",
        "Filename": f"file_{msg_id}",
        "Expire_Timestamp": (datetime.now() + timedelta(days=30)).isoformat(),
        "channelId": f"channel_{random.randint(1, 10)}"
    }


def run_tests(datastore_handler):
    """Test functionality of the DatastoreHandler class."""
    print("Starting tests for DatastoreHandler...")

    # Test pushing a single message
    print("\nTest 1: Push a single message")
    msg_id = "test_msg_1"
    message = generate_sample_message(msg_id)
    datastore_handler.push_msg(message)
    print(f"Pushed message with ID: {msg_id}")

    # Test retrieving the pushed message
    print("\nTest 2: Retrieve the pushed message")
    retrieved_message = datastore_handler.get_msg_by_id(msg_id)
    if retrieved_message:
        print(f"Retrieved message: {retrieved_message}")
    else:
        print(f"Failed to retrieve message with ID: {msg_id}")

    # Test adding a runtime to the message
    print("\nTest 3: Add runtime to the message")
    runtime_tuple = (msg_id, random.uniform(10, 20))
    datastore_handler.add_runtime_to_message(runtime_tuple)
    updated_message = datastore_handler.get_msg_by_id(msg_id)
    print(f"Updated message with runtime: {updated_message.get('Runtime')}")

    # Test batch pushing messages
    print("\nTest 4: Batch push messages")
    batch_messages = [generate_sample_message(f"test_msg_{i}") for i in range(2, 5)]
    datastore_handler.push_batch_msgs(batch_messages)
    print(f"Pushed batch of messages with IDs: {[msg['Id'] for msg in batch_messages]}")

    # Test retrieving all message IDs
    print("\nTest 5: Retrieve all message IDs")
    all_msg_ids = datastore_handler.get_all_msg_ids()
    print(f"Retrieved message IDs: {all_msg_ids}")

    # Test batch adding runtimes
    print("\nTest 6: Batch add runtimes to messages")
    batch_runtimes = [
        (f"test_msg_{i}", random.uniform(10, 20))
        for i in range(1, 5)
    ]
    datastore_handler.push_batch_runtimes(batch_runtimes)
    for runtime in batch_runtimes:
        print(runtime)
        msg_id = runtime[0]
        msg_with_runtime = datastore_handler.get_msg_by_id(msg_id)
        print(f"Message {msg_id} with runtime: {msg_with_runtime.get('Runtime')}")

    print("\nTests completed.")


if __name__ == "__main__":
    def log_item(message):
        """Log messages to the console."""
        print(message)

    # Initialize the DatastoreHandler with a logger
    datastore_handler = DatastoreHandler(log_item=log_item)

    # Run the tests
    run_tests(datastore_handler)
