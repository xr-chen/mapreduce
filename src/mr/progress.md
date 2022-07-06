## TODO List

- split the task given by the user then send them to `N` workers(mapper).
- do we actually need to sort the intermediate result by keys?
- will server method in coordinator.go automatically launch a thread?
- do reducer have to do reduce work after mapping stage is finished.

- [ ] how to know the id of a machine
- [ ] using wait groups to tell the worker to stop
- [ ] map worker works so slow