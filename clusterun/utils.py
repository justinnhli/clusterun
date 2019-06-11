from importlib.util import spec_from_file_location, module_from_spec


def load_name(path, name):
    """Import an identifier from another module.

    Arguments:
        path (Path): The path of the file to import from.
        name (str): The name of the variable to import.

    Returns:
        Any: The value of the identifier in that module
    """
    specification = spec_from_file_location(path.name, path)
    module = module_from_spec(specification)
    specification.loader.exec_module(module)
    return getattr(module, name)
