
import json
import os


class Document:

    def __init__(self, document):
        self.document = document

    def get(self):
        return Document(self.document)

    def to_dict(self):
        return self.document


class Collection:

    def __init__(self, collection=None, coll_list=None):
        self.collection = collection
        self.coll_list = coll_list

    def document(self, document_path):
        return Document(self.collection[document_path])

    def get(self):
        return [val for val in self.collection.values()]

    def where(self, field, logical, value):
        ret = {}
        for doc, item in dict(self.collection).items():
            if logical == ">=":
                if item[field] >= value:
                    ret[doc] = item

            if logical == "<=":
                if item[field] <= value:
                    ret[doc] = item

        return Collection(ret)

    def order_by(self, column, direction=None):

        #placeholder, just assuming descending for this implemention
        vals = self.get()
        if direction is not None:
            self.coll_list = sorted(vals, key=lambda i: i[column], reverse=True)

        else:
            self.coll_list = sorted(vals, key=lambda i: i[column])

        return Collection(self.collection, self.coll_list)

    def stream(self):

        return [Document(val) for val in self.coll_list]

    def limit(self, limit):
        self.coll_list = self.coll_list[:limit]

        return Collection(self.collection, self.coll_list)


class Client:

    def __init__(self):
        pass

    def collection(self, collection_path):
        my_path = os.path.abspath(os.path.dirname(__file__))
        base_dir = os.path.join(my_path, 'firestore', collection_path + '.json')
        with open(base_dir) as p:
            coll = json.loads(p.read())[collection_path]

        return Collection(coll)
