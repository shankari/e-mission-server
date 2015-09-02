import logging
import attrdict as ad
import enum as enum
import collections as coll

class WrapperBase(ad.AttrDict):
  """
  Base for our classes.
  Enhancements over AttrDict:
    - Specifies a list of valid properties. This is definitely NOT pythonic,
      but helps with self-documenting code. Otherwise, people want to use the
      class and they have no idea what the properties are. This also enables
      the following nice properties:
        - Read-only properties. We can mark the valid properties as read-only,
          which means that we can 
        - Support inline completion in ipython (my favorite feature!).
        - Support dependencies (properties that are automatically populated
          based on other properties for precomputation)
  """
  """
    All subclasses should define:
    - props: a map of valid properties, with a value indicating whether
      they are read-only or not
    - _populateDependencies: a method to pre-populate fields based on the
      current field. This can be empty, and implemented by pass
  """
  Access = enum.Enum("PropertyAccess", "RO RW")


  def __init__(self, *args, **kwargs):
    super(WrapperBase, self).__init__(*args, **kwargs)
    self._populateDependencies()

  def __dir__(self):
    return dir(super) + self.props.keys()

  def __getattr__(self, key):
    # logging.debug("__getattr__ called for key = %s" % key)

    if key in self.props:
        # This code is copied from the base Attr code.
        # This allows us to pass the key into _build as well instead of only the value
        if key not in self or not self._valid_name(key):
            raise AttributeError(
                "'{cls}' instance has no attribute '{name}'".format(
                    cls=self.__class__.__name__, name=key
                )
            )
        # logging.debug("Returning self._build")
        return self._build(key, self[key])
    else:
        raise AttributeError("property %s is not defined for %s" % (key, self.__class__.__name__))

  def __setattr__(self, key, value):
    if key in self.props:
        if self.props[key] == WrapperBase.Access.RW:
            return super(WrapperBase, self).__setattr__(key, value)
        else:
            raise AttributeError("property %s is read-only" % key)
    else:
        raise AttributeError("property %s is not defined for %s" % (key, self.__class__.__name__))

  def __repr__(self):
    """
    We would like to use a name for the class that is different from _AttrDict_.
    Unfortunately, the AttrDict name is hardcoded in the parent class, which
    makes sense since they don't want to do a lot of setup, but it means that we
    can't easily outgrow it.

    Out of the three options:
    1. Modify the parent class
    2. Duplicate the parent class
    3. Replace the string here, we have chosen to go with (3) for greatest cleanliness.
    """
    parentResult = super(WrapperBase, self).__repr__()
    return parentResult.replace("AttrDict", self.__class__.__name__)

  def __call__(self, key):
    print("_call called with %s, %s" % (self, key))
    return super(WrapperBase, self).__call__(key)

  @staticmethod
  def _get_class(wrapper_name):
    import importlib

    # Convention is to name the module with the wrapper name in lower case, and
    # then have a class in the module with the name.
    # e.g. The Location class will be in a module named location.
    wrapperModule = importlib.import_module("emission.core.wrapper.%s" % wrapper_name)
    wrapperClassName = wrapper_name[0].upper() + wrapper_name[1:]
    return getattr(wrapperModule, wrapperClassName)

  def _build(self, key, obj):
    # logging.debug("_build called with %s, %s, %s" % (self, key, obj))
    if isinstance(obj, coll.Mapping):
        key_class = self._get_class(key)
        # logging.debug("key_class = %s" % key_class)
        return key_class._constructor(obj, self._configuration())
    else:
        return super(WrapperBase, self)._build(obj)
